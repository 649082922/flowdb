from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable
from urllib.parse import quote_plus

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, Float, Identity, Integer, LargeBinary,
    MetaData, Numeric, String, Table, Text, bindparam, create_engine, func, inspect, null, select, text,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql.sqltypes import BINARY, BLOB, CHAR, CLOB, DECIMAL, DOUBLE, FLOAT, INT, NCHAR, NVARCHAR, REAL, SMALLINT, VARCHAR

from .models import ConnectionConfig


def engine_url(config: ConnectionConfig) -> str:
    user = quote_plus(config.username)
    password = quote_plus(config.password)
    host = config.host.strip("[]")
    if config.type == "oracle":
        service = quote_plus(config.database)
        return f"oracle+oracledb://{user}:{password}@{host}:{config.port}/?service_name={service}"
    database = quote_plus(config.database)
    if config.type in {"mysql", "tdsql"}:
        return f"mysql+pymysql://{user}:{password}@{host}:{config.port}/{database}?charset=utf8mb4"
    return f"postgresql+psycopg://{user}:{password}@{host}:{config.port}/{database}"


def make_engine(config: ConnectionConfig) -> Engine:
    connect_args = {"tcp_connect_timeout": 10} if config.type == "oracle" else {"connect_timeout": 10}
    return create_engine(engine_url(config), pool_pre_ping=True, pool_recycle=1800, connect_args=connect_args)


def default_schema(config: ConnectionConfig) -> str | None:
    if config.schema_name:
        return config.schema_name
    if config.type == "oracle":
        return config.username.upper()
    if config.type == "postgresql":
        return "public"
    return None


def test_engine(engine: Engine) -> tuple[str, str]:
    with engine.connect() as connection:
        dialect = engine.dialect.name
        version = ".".join(str(part) for part in (engine.dialect.server_version_info or ()))
        connection.execute(text("SELECT 1 FROM DUAL" if dialect == "oracle" else "SELECT 1"))
    return dialect, version or "unknown"


def table_name_case_capabilities(engine: Engine) -> dict[str, Any]:
    """Return the target server's table-name case behavior.

    ``lower_case_table_names`` is a server initialization setting.  The
    migration tool only observes it; it must never imply that a task can
    change the TDSQL/MySQL server setting.
    """
    if engine.dialect.name != "mysql":
        return {
            "lower_case_table_names": None,
            "table_name_case_sensitive": None,
        }
    with engine.connect() as connection:
        value = int(connection.execute(text("SELECT @@global.lower_case_table_names")).scalar_one())
    return {
        "lower_case_table_names": value,
        "table_name_case_sensitive": value == 0,
    }


def resolve_table_name_policy(
    requested: str, lower_case_table_names: int | None
) -> str:
    if requested not in {"auto", "preserve", "lower", "upper"}:
        raise ValueError(f"不支持的目标对象命名策略：{requested}")
    if requested == "auto":
        return "lower" if lower_case_table_names == 1 else "preserve"
    if requested in {"preserve", "upper"} and lower_case_table_names == 1:
        raise ValueError(
            "目标 TDSQL/MySQL 的 lower_case_table_names=1，表名会以小写存储，不能保留源端大小写或统一大写；请使用“自动适配”或“统一小写”"
        )
    return requested


def apply_table_name_policy(name: str, resolved_policy: str) -> str:
    """Map only the object component, preserving an optional schema prefix."""
    prefix, separator, object_name = name.rpartition(".")
    if not separator:
        prefix, object_name = "", name
    if resolved_policy == "lower":
        object_name = object_name.lower()
    elif resolved_policy == "upper":
        object_name = object_name.upper()
    return f"{prefix}.{object_name}" if prefix else object_name


def build_target_table_name_map(
    names: Iterable[str], resolved_policy: str, lower_case_table_names: int | None
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    seen: dict[str, str] = {}
    for source_name in names:
        target_name = apply_table_name_policy(source_name, resolved_policy)
        collision_key = target_name.casefold() if lower_case_table_names in {1, 2} else target_name
        previous = seen.get(collision_key)
        if previous is not None and previous != source_name:
            raise ValueError(
                f"目标对象名冲突：源对象 {previous} 和 {source_name} 都会映射为 {target_name}"
            )
        seen[collision_key] = source_name
        mapping[source_name] = target_name
    return mapping


def list_tables(engine: Engine, schema: str | None, owners: list[str] | None = None) -> list[dict[str, Any]]:
    inspector = inspect(engine)
    result = []
    effective_schemas = owners or ([schema] if schema else [None])
    for effective in effective_schemas:
        for name in inspector.get_table_names(schema=effective):
            columns = inspector.get_columns(name, schema=effective)
            pk = inspector.get_pk_constraint(name, schema=effective).get("constrained_columns") or []
            result.append({"schema_name": effective, "name": name, "columns": len(columns), "primary_keys": pk})
    return result


def list_objects(engine: Engine, schema: str | None, owners: list[str] | None = None) -> list[dict[str, Any]]:
    dialect = engine.dialect.name
    if dialect == "oracle":
        return _list_oracle_objects_bulk(engine, schema, owners)

    inspector = inspect(engine)
    result = []
    effective_schemas = owners or ([schema] if schema else [None])
    for effective in effective_schemas:
        for object_type, names in (("table", inspector.get_table_names(schema=effective)), ("view", inspector.get_view_names(schema=effective))):
            for name in names:
                columns = inspector.get_columns(name, schema=effective)
                primary_keys = [] if object_type == "view" else (inspector.get_pk_constraint(name, schema=effective).get("constrained_columns") or [])
                result.append({"schema_name": effective, "name": name, "columns": len(columns), "primary_keys": primary_keys, "object_type": object_type})
    return sorted(result, key=lambda item: (item["object_type"], item["name"].casefold()))


def _list_oracle_objects_bulk(
    engine: Engine, schema: str | None, owners: list[str] | None = None
) -> list[dict[str, Any]]:
    """Read Oracle object metadata in four round trips, independent of table count.

    SQLAlchemy's inspector executes column and primary-key queries once per
    object.  Forty-eight tables therefore caused more than one hundred network
    round trips.  Oracle's ALL_* dictionary views can return the same metadata
    in bulk and also classify partition tables reliably.
    """
    selected_owners = [str(item).strip().upper() for item in (owners or []) if str(item).strip()]
    if not selected_owners and schema:
        selected_owners = [str(schema).strip().upper()]
    if not selected_owners:
        raise ValueError("读取 Oracle 对象时必须指定 owner/schema")

    owner_filter = bindparam("owners", expanding=True)
    object_sql = text(
        """
        SELECT t.owner, t.table_name AS object_name,
               CASE WHEN p.table_name IS NULL THEN 'table' ELSE 'partitioned_table' END AS object_type
          FROM all_tables t
          LEFT JOIN all_part_tables p
            ON p.owner = t.owner AND p.table_name = t.table_name
         WHERE UPPER(t.owner) IN :owners
        UNION ALL
        SELECT v.owner, v.view_name AS object_name, 'view' AS object_type
          FROM all_views v
         WHERE UPPER(v.owner) IN :owners
        """
    ).bindparams(owner_filter)
    columns_sql = text(
        """
        SELECT owner, table_name AS object_name, COUNT(*) AS column_count
          FROM all_tab_columns
         WHERE UPPER(owner) IN :owners
         GROUP BY owner, table_name
        """
    ).bindparams(bindparam("owners", expanding=True))
    primary_keys_sql = text(
        """
        SELECT c.owner, c.table_name AS object_name, cc.column_name, cc.position
          FROM all_constraints c
          JOIN all_cons_columns cc
            ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
         WHERE c.constraint_type = 'P' AND UPPER(c.owner) IN :owners
         ORDER BY c.owner, c.table_name, cc.position
        """
    ).bindparams(bindparam("owners", expanding=True))
    sequences_sql = text(
        """
        SELECT sequence_owner AS owner, sequence_name AS object_name
          FROM all_sequences
         WHERE UPPER(sequence_owner) IN :owners
        """
    ).bindparams(bindparam("owners", expanding=True))

    with engine.connect() as connection:
        object_rows = list(connection.execute(object_sql, {"owners": selected_owners}).mappings())
        column_rows = list(connection.execute(columns_sql, {"owners": selected_owners}).mappings())
        pk_rows = list(connection.execute(primary_keys_sql, {"owners": selected_owners}).mappings())
        sequence_rows = list(connection.execute(sequences_sql, {"owners": selected_owners}).mappings())

    def key(owner: Any, name: Any) -> tuple[str, str]:
        return str(owner).upper(), str(name).upper()

    column_counts = {
        key(row["owner"], row["object_name"]): int(row["column_count"] or 0)
        for row in column_rows
    }
    primary_keys: dict[tuple[str, str], list[str]] = {}
    for row in pk_rows:
        primary_keys.setdefault(key(row["owner"], row["object_name"]), []).append(
            str(row["column_name"])
        )

    result = []
    for row in object_rows:
        owner, name = str(row["owner"]), str(row["object_name"])
        object_type = str(row["object_type"]).lower()
        result.append(
            {
                "schema_name": owner,
                "name": name,
                "columns": column_counts.get(key(owner, name), 0),
                "primary_keys": [] if object_type == "view" else primary_keys.get(key(owner, name), []),
                "object_type": object_type,
            }
        )
    result.extend(
        {
            "schema_name": str(row["owner"]),
            "name": str(row["object_name"]),
            "columns": 0,
            "primary_keys": [],
            "object_type": "sequence",
        }
        for row in sequence_rows
    )
    return sorted(result, key=lambda item: (item["object_type"], item["name"].casefold()))


def list_owners(engine: Engine, config: ConnectionConfig) -> list[str]:
    """返回源端可访问的 owner/schema 列表，供前端“筛选 owner”下拉使用。

    Oracle 优先查 all_tables 去重（含当前用户可访问的所有表 owner）；
    PostgreSQL 返回非系统 schema；MySQL 直接返回连接默认库名。
    """
    dialect = engine.dialect.name
    try:
        if dialect == "oracle":
            with engine.connect() as connection:
                rows = connection.execute(text("SELECT DISTINCT owner FROM all_tables ORDER BY owner"))
                owners = [str(row[0]) for row in rows if row[0]]
            return owners
        if dialect == "postgresql":
            with engine.connect() as connection:
                rows = connection.execute(text(
                    "SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema') "
                    "AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp%' ORDER BY nspname"
                ))
                owners = [str(row[0]) for row in rows if row[0]]
            return owners
    except Exception:
        # MySQL 或元数据视图无权限时，退回连接默认 schema
        pass
    fallback = default_schema(config)
    return [fallback] if fallback else []


def portable_type(source_type: Any, target_dialect: str | None = None, identity: bool = False):
    length = getattr(source_type, "length", None)
    precision = getattr(source_type, "precision", None)
    scale = getattr(source_type, "scale", None)
    name = source_type.__class__.__name__.upper()
    # SQLAlchemy models Oracle BINARY_FLOAT/BINARY_DOUBLE as numeric types,
    # so handle them before the generic NUMBER/DECIMAL branch.
    if name in {"BINARY_FLOAT", "BINARY_DOUBLE"} or isinstance(source_type, (DOUBLE, FLOAT, REAL, Float)):
        if target_dialect == "mysql":
            return mysql.DOUBLE(asdecimal=False)
        return Float()
    if isinstance(source_type, (DECIMAL, Numeric)) or name in {"NUMBER", "MONEY"}:
        if identity:
            return BigInteger()
        if target_dialect == "mysql" and name == "NUMBER" and precision is not None:
            # 腾讯云 Oracle 迁移规则：带明确精度且无小数的 NUMBER 应按可容纳
            # 的最小整型转换，避免 Java 侧把业务标志/ID 读成 BigDecimal。
            # NUMBER(p, negative_scale) 的整数位数为 p - s。
            numeric_scale = int(scale or 0)
            if numeric_scale <= 0:
                integer_digits = int(precision) - numeric_scale
                if integer_digits < 3:
                    return mysql.TINYINT()
                if integer_digits < 5:
                    return mysql.SMALLINT()
                if integer_digits < 10:
                    return Integer()
                if integer_digits < 19:
                    return BigInteger()
                if integer_digits <= 65:
                    return mysql.NUMERIC(precision=integer_digits, scale=0)
                # TDSQL NUMERIC 最大精度 65；Oracle 负 scale 可能产生超过
                # 65 位的整数范围，改存字符可避免静默溢出。
                return String(128)
            # MySQL/TDSQL 要求 scale <= precision；Oracle 允许 NUMBER(4,5)。
            mysql_precision = max(int(precision), numeric_scale)
            if mysql_precision <= 65 and numeric_scale <= 30:
                return mysql.NUMERIC(precision=mysql_precision, scale=numeric_scale)
            # 目标端实测 NUMERIC scale 上限为 30；超出时使用字符串保留原值，
            # 不截断多余小数位。
            return String(128)
        if precision and precision <= 38:
            return Numeric(precision=precision, scale=scale or 0)
        if target_dialect == "mysql":
            # Bare Oracle NUMBER has dynamic precision/scale. MySQL's bare
            # DECIMAL silently becomes DECIMAL(10,0), so choose its widest
            # practical exact representation instead of rounding fractions.
            # scale=0: 无精度 NUMBER 通常为 ID/整数列，避免 numeric(65,30)
            # 显示成 1.000...000 假小数；Oracle 最大 38 位整数，65 位足够。
            # TDSQL 定点型支持 DECIMAL 与 NUMERIC（两者等价），统一用 NUMERIC。
            return mysql.NUMERIC(precision=65, scale=0)
        return Numeric()
    if isinstance(source_type, BigInteger) or name == "BIGINT":
        return BigInteger()
    if isinstance(source_type, (SMALLINT, INT, Integer)) or name in {"TINYINT", "MEDIUMINT"}:
        return Integer()
    if isinstance(source_type, Boolean) or name == "BOOLEAN":
        return Boolean()
    if name == "BIT":
        # BIT(1) is a boolean, BIT(n>1) is a bit vector that must survive as bytes.
        bit_length = getattr(source_type, "length", None)
        if bit_length in (None, 1):
            return Boolean()
        return LargeBinary(length=(int(bit_length) + 7) // 8)
    if isinstance(source_type, DateTime) or "TIMESTAMP" in name:
        timezone = bool(getattr(source_type, "timezone", False))
        if target_dialect == "mysql":
            return String(48) if timezone else mysql.DATETIME(fsp=6)
        return DateTime(timezone=timezone)
    if isinstance(source_type, Date):
        return Date()
    if name in {"RAW", "VARBINARY"}:
        if target_dialect == "mysql":
            return mysql.VARBINARY(length=length or 2000)
        return LargeBinary(length=length or 2000)
    if isinstance(source_type, (BLOB, BINARY, LargeBinary)) or name in {"BYTEA", "LONGBLOB", "LONG RAW", "LONG_RAW"}:
        # LONG RAW 是 Oracle 二进制类型（最大 2GB），映射为二进制列而非字符列，
        # 否则插入 \x80 等高位字节会报 1366 Incorrect string value。
        if target_dialect == "mysql" and (name in {"BLOB", "LONGBLOB", "BYTEA", "LONG RAW", "LONG_RAW"} or not length or length > 65535):
            return mysql.LONGBLOB()
        return LargeBinary(length=length)
    if isinstance(source_type, (CLOB, Text)) or name in {"LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "NCLOB", "XMLTYPE"}:
        if target_dialect == "mysql":
            return mysql.LONGTEXT()
        return Text()
    if name in {"JSON", "JSONB"}:
        # TDSQL 支持原生 JSON 类型（文档：支持存储 Json 格式的数据类型），
        # 迁移为原生 JSON 而非文本，保留 JSON 校验/查询语义。
        if target_dialect == "mysql":
            return mysql.JSON()
        return Text()
    if isinstance(source_type, (CHAR, NCHAR)) and length:
        if int(length) <= 255:
            return mysql.CHAR(length=int(length)) if target_dialect == "mysql" else CHAR(length=int(length))
        if target_dialect == "mysql":
            return mysql.LONGTEXT()
        return Text()
    if isinstance(source_type, (VARCHAR, NVARCHAR, String)) and length:
        # TDSQL 文档：VARCHAR 最大 65,535，CHAR 0~255。
        # 超长字符列降级为 LONGTEXT（文档虽不建议 LOB/TEXT，但为保数据完整）。
        if length > 65535:
            if target_dialect == "mysql":
                return mysql.LONGTEXT()
            return Text()
        return String(length=int(length))

    # --- P1 extension: special / exotic types with explicit degradation ---
    # XML
    if name in {"XML", "XMLTYPE"} or "XML" in str(source_type).upper():
        if target_dialect == "mysql":
            return mysql.LONGTEXT()
        return Text()

    # Spatial types (Oracle SDO_GEOMETRY / MySQL GEOMETRY family / PG PostGIS geometry|geography)
    _geometry_names = {
        "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT",
        "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION",
        "SDO_GEOMETRY", "GEOGRAPHY",
    }
    _type_str_upper = str(source_type).upper()
    if name in _geometry_names or "SDO_GEOMETRY" in _type_str_upper or "GEOMETRY" in _type_str_upper or "GEOGRAPHY" in _type_str_upper:
        if target_dialect == "mysql":
            try:
                from sqlalchemy.dialects.mysql import GEOMETRY as MySQLGeometry

                return MySQLGeometry()
            except Exception:
                return LargeBinary()
        return Text()

    # MySQL ENUM / SET
    if name == "ENUM":
        if target_dialect == "mysql":
            return source_type
        enum_length = getattr(source_type, "length", None)
        if enum_length:
            return String(length=min(int(enum_length), 10485760))
        return String(length=255)
    if name == "SET":
        if target_dialect == "mysql":
            return source_type
        return String(length=1024)

    # PostgreSQL UUID / ARRAY
    if name == "UUID":
        if target_dialect == "postgresql":
            from sqlalchemy import UUID as _SqlUUID

            return _SqlUUID()
        if target_dialect == "oracle":
            return CHAR(36)
        return String(length=36)
    if name == "ARRAY":
        return Text()

    # Oracle INTERVAL
    if name == "INTERVAL":
        return String(length=48)

    return Text()


def portable_type_info(
    source_type: Any,
    target_dialect: str | None = None,
    identity: bool = False,
) -> dict[str, Any]:
    """portable_type 的扩展版本：返回目标类型并附降级标记与说明。

    ``target_type`` 与 portable_type 返回的类型完全一致（保持兼容），
    额外的 ``degraded`` / ``degradation`` 供评估报告标注类型映射风险。
    """
    mapped = portable_type(source_type, target_dialect, identity)
    name = source_type.__class__.__name__.upper()
    type_str = str(source_type).upper()
    length = getattr(source_type, "length", None)
    degraded = False
    degradation: str | None = None

    if name == "NUMBER" and target_dialect == "mysql" and isinstance(mapped, String):
        degraded = True
        degradation = (
            "Oracle NUMBER 的精度/小数位超出 TDSQL NUMERIC(65,30) 范围，"
            "已改为 VARCHAR(128) 以保留文本值；目标端不能直接进行数值运算"
        )
    elif "TIMESTAMP" in name and bool(getattr(source_type, "timezone", False)):
        degraded = target_dialect == "mysql"
        degradation = (
            "TDSQL DATETIME 不保存时区，已映射为 VARCHAR(48) 保留时区偏移"
            if target_dialect == "mysql"
            else None
        )
    elif name == "BFILE" or "BFILE" in type_str:
        degraded = True
        degradation = "BFILE 仅保存 Oracle 外部文件引用，文件内容不在数据库内，迁移列将置 NULL"
    elif name in {"XML", "XMLTYPE"} or "XML" in type_str:
        degraded = True
        degradation = "XML 类型映射为目标端文本类型，XML 结构与查询语义需在应用层处理"
    elif name in {"GEOMETRY", "SDO_GEOMETRY", "GEOGRAPHY"} or "GEOMETRY" in type_str or "GEOGRAPHY" in type_str:
        degraded = target_dialect != "mysql"
        degradation = (
            "空间类型在 MySQL 端映射为原生几何类型，空间索引需在目标端重建"
            if target_dialect == "mysql"
            else "空间类型映射为文本/二进制，空间函数与索引语义需在目标端重建"
        )
    elif name == "ENUM":
        degraded = target_dialect != "mysql"
        degradation = (
            "ENUM 在 MySQL 端保留原枚举；其他端映射为字符串，取值集合需应用层校验"
            if target_dialect == "mysql"
            else "ENUM 枚举映射为字符串，取值集合需应用层校验"
        )
    elif name == "SET":
        degraded = target_dialect != "mysql"
        degradation = (
            "SET 在 MySQL 端保留原集合；其他端映射为字符串，位集合语义需应用层处理"
            if target_dialect == "mysql"
            else "SET 映射为字符串，位集合语义需应用层处理"
        )
    elif name == "BIT":
        bit_length = length if length is not None else 1
        if int(bit_length) <= 1:
            degraded = False
            degradation = None
        else:
            degraded = True
            degradation = "多比特 BIT(n) 映射为二进制大字段，按位语义需应用层处理"
    elif name == "UUID":
        if target_dialect == "postgresql":
            degraded = False
            degradation = None
        else:
            degraded = True
            degradation = "UUID 映射为定长字符串，排序/比较语义与 PG 原生 UUID 基本一致"
    elif name == "ARRAY":
        degraded = True
        degradation = "PG 数组映射为文本（字面量），数组查询/索引语义需在目标端重建"
    elif name == "INTERVAL":
        degraded = True
        degradation = "INTERVAL 映射为文本，目标端无法直接参与日期时间运算"
    elif name in {"RAW", "VARBINARY"}:
        degraded = True
        degradation = "RAW/VARBINARY 映射为二进制类型，注意字节长度上限与字符集无关"
    elif name in {"NCHAR", "NVARCHAR", "NVARCHAR2"}:
        degraded = target_dialect != "oracle"
        degradation = (
            "NCHAR/NVARCHAR2 映射为普通字符串，需确认目标端字符集可容纳全量字符"
            if target_dialect != "oracle"
            else None
        )
    return {"target_type": mapped, "degraded": degraded, "degradation": degradation}


def _probe_number_scale(source_engine: Engine, source_schema: str | None, table_name: str, columns: list[dict[str, Any]]) -> set[str]:
    """Oracle 裸 NUMBER 列采样：若实际数据含小数，返回需要保留小数位的列名集合。

    裸 NUMBER（无 precision/scale）默认映射为 NUMERIC(65,0)，可避免 id 类整数列
    显示成 1.000...000 假小数；但若数据本身含小数（如 0.01），scale=0 会截断丢失。
    此处用 MOD(col,1) 探测数据中是否存在非零小数部分。
    """
    need_scale: set[str] = set()
    if source_engine.dialect.name != "oracle":
        return need_scale
    bare = [col["name"] for col in columns if col["type"].__class__.__name__.upper() == "NUMBER" and getattr(col["type"], "precision", None) is None]
    if not bare:
        return need_scale
    owner = (source_schema or "").upper()
    quoted = source_engine.dialect.identifier_preparer
    qualified = f"{quoted.quote_schema(owner)}.{quoted.quote(table_name)}"
    try:
        with source_engine.connect() as connection:
            for colname in bare:
                c = quoted.quote(colname)
                sql = text(
                    f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {qualified} "
                    f"WHERE MOD({c}, 1) <> 0 AND {c} IS NOT NULL) THEN 1 ELSE 0 END FROM dual"
                )
                if connection.execute(sql).scalar():
                    need_scale.add(colname)
    except Exception:
        # 探测失败时保持默认 scale=0，不阻塞建表
        pass
    return need_scale


def _resolve_oracle_column_types(source_engine: Engine, source_schema: str | None, table_name: str, columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Oracle 方言对 LONG RAW 等类型反射为 NullType，portable_type 会将其降级为
    Text()，导致高位字节插入目标端报 1366 Incorrect string value。这里用数据字典
    all_tab_columns 补全真实类型，把 LONG RAW 纠正为二进制类型。"""
    owner = (source_schema or "").upper()
    try:
        with source_engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT column_name, data_type FROM all_tab_columns "
                    "WHERE owner = :owner AND table_name = :table_name"
                ),
                {"owner": owner, "table_name": table_name.upper()},
            ).fetchall()
    except Exception:
        return columns
    real_types = {str(row[0]).upper(): str(row[1]).upper() for row in rows}
    for col in columns:
        real = real_types.get(str(col["name"]).upper())
        if real == "LONG RAW" and col["type"].__class__.__name__.upper() in {"NULLTYPE", "TEXT"}:
            col["type"] = LargeBinary()
    return columns


def build_target_table(source_engine: Engine, target_engine: Engine, source_schema: str | None, target_schema: str | None, table_name: str, target_table_name: str | None = None) -> tuple[Table, list[str]]:
    inspector = inspect(source_engine)
    columns = inspector.get_columns(table_name, schema=source_schema)
    if not columns:
        raise RuntimeError(f"源表不存在或无列：{table_name}")
    if source_engine.dialect.name == "oracle":
        columns = _resolve_oracle_column_types(source_engine, source_schema, table_name, columns)
        need_number_scale = _probe_number_scale(source_engine, source_schema, table_name, columns)
    else:
        need_number_scale = set()
    try:
        primary_keys = set(inspector.get_pk_constraint(table_name, schema=source_schema).get("constrained_columns") or [])
    except Exception:
        # Views do not expose primary-key metadata on every driver.
        primary_keys = set()
    metadata = MetaData()
    target_columns = []
    for column in columns:
        primary_key = column["name"] in primary_keys
        identity_info = column.get("identity") or {}
        is_identity = bool(identity_info) or column.get("autoincrement") is True
        column_args: list[Any] = [column["name"], portable_type(column["type"], target_engine.dialect.name, is_identity)]
        if column["name"] in need_number_scale and target_engine.dialect.name == "mysql":
            # 裸 NUMBER 且数据含小数：保留小数位，避免 scale=0 截断
            column_args[1] = mysql.NUMERIC(precision=65, scale=30)
        if is_identity and target_engine.dialect.name in {"oracle", "postgresql"}:
            column_args.append(Identity(start=identity_info.get("start"), increment=identity_info.get("increment"), always=False))
        target_columns.append(Column(*column_args, nullable=column.get("nullable", True), primary_key=primary_key, autoincrement=is_identity if primary_key else "auto"))
    if not primary_keys and target_engine.dialect.name == "mysql":
        # TDSQL/MySQL commonly enforces sql_require_primary_key. Give a source
        # view or heap table an internal target key without changing its data.
        target_columns.insert(0, Column("__flowdb_row_id", BigInteger(), primary_key=True, autoincrement=True))
    return Table(target_table_name or table_name, metadata, *target_columns, schema=target_schema), [column["name"] for column in columns]


def reflect_source_object(
    source_engine: Engine, source_schema: str | None, object_name: str
) -> Table:
    """Reflect Oracle tables/views without assuming one identifier casing.

    SQLAlchemy normalizes ordinary Oracle identifiers to lowercase, while an
    explicitly created or quoted view can be returned in uppercase.  Trying
    only the lowercase spelling made those views fail with an opaque
    ``SCHEMA.object`` NoSuchTableError.
    """
    candidates = list(dict.fromkeys((object_name, object_name.lower(), object_name.upper())))
    failures: list[str] = []
    for candidate in candidates:
        try:
            return Table(
                candidate,
                MetaData(),
                autoload_with=source_engine,
                schema=source_schema,
            )
        except (NoSuchTableError, KeyError) as exc:
            failures.append(f"{candidate}: {exc.__class__.__name__}: {exc}")
    attempted = "、".join(candidates)
    detail = "；".join(failures)
    raise RuntimeError(
        f"源端对象反射失败：{source_schema or '<默认Schema>'}.{object_name}；"
        f"已尝试名称 {attempted}；原始异常：{detail}"
    )


# ---------------------------------------------------------------------------
# Oracle 分区表识别 / 分区信息采集 / TDSQL 分区子句构建
# ---------------------------------------------------------------------------

def get_oracle_partition_info(engine: Engine, schema: str | None, table_name: str) -> dict[str, Any] | None:
    """采集 Oracle 分区表元数据，供 TDSQL 分区子句转换与迁移阶段排序使用。

    返回 dict 或 None（非分区表/查询失败）。字段：
    - partitioning_type: RANGE/LIST/HASH 等
    - subpartitioning_type: 复合分区子分区类型（如 HASH/LIST）或 None
    - partition_count / def_subpartition_count: int
    - interval: "YES"/"NO"
    - partition_key_columns: list[str]（按 column_position 排序）
    - subpartition_key_columns: list[str]
    - partitions: list[{name, high_value}]（按 partition_position 排序）
    - subpartitions: list[str]
    """
    owner = (schema or "").upper()
    table_upper = table_name.upper()
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT partitioning_type, subpartitioning_type, partition_count, "
                    "def_subpartition_count, interval FROM all_part_tables "
                    "WHERE owner = :owner AND table_name = :table"
                ),
                {"owner": owner, "table": table_upper},
            ).fetchone()
            if not row:
                return None
            info: dict[str, Any] = {
                "partitioning_type": str(row[0] or "").upper() or None,
                "subpartitioning_type": str(row[1] or "").upper() or None,
                "partition_count": int(row[2] or 0),
                "def_subpartition_count": int(row[3] or 0),
                # Oracle all_part_tables.interval 对间隔分区表存间隔定义文本（如
                # NUMTOYMINTERVAL(1,'MONTH')），非间隔分区为 NULL；非空即视为间隔分区。
                "interval": "YES" if str(row[4] or "").strip() else "NO",
            }
            key_rows = connection.execute(
                text(
                    "SELECT column_name FROM all_part_key_columns "
                    "WHERE owner = :owner AND name = :table ORDER BY column_position"
                ),
                {"owner": owner, "table": table_upper},
            ).fetchall()
            info["partition_key_columns"] = [str(r[0]) for r in key_rows if r[0]]
            part_rows = connection.execute(
                text(
                    "SELECT partition_name, high_value FROM all_tab_partitions "
                    "WHERE table_owner = :owner AND table_name = :table "
                    "ORDER BY partition_position"
                ),
                {"owner": owner, "table": table_upper},
            ).fetchall()
            info["partitions"] = [
                {"name": str(r[0]), "high_value": str(r[1]) if r[1] is not None else None}
                for r in part_rows
            ]
            # 子分区元数据视图（all_subpartition_key_columns / all_tab_subpartitions）
            # 在部分 Oracle 环境中对普通用户无权限/不存在，失败时不阻塞主流程。
            sub_key_rows = []
            try:
                sub_key_rows = connection.execute(
                    text(
                        "SELECT column_name FROM all_subpartition_key_columns "
                        "WHERE owner = :owner AND name = :table ORDER BY column_position"
                    ),
                    {"owner": owner, "table": table_upper},
                ).fetchall()
            except Exception:
                sub_key_rows = []
            info["subpartition_key_columns"] = [str(r[0]) for r in sub_key_rows if r[0]]
            sub_part_rows = []
            try:
                sub_part_rows = connection.execute(
                    text(
                        "SELECT subpartition_name FROM all_tab_subpartitions "
                        "WHERE table_owner = :owner AND table_name = :table "
                        "ORDER BY subpartition_position"
                    ),
                    {"owner": owner, "table": table_upper},
                ).fetchall()
            except Exception:
                sub_part_rows = []
            info["subpartitions"] = [str(r[0]) for r in sub_part_rows if r[0]]
            return info
    except Exception:
        # 元数据视图无权限或查询失败时不阻塞，按普通表处理
        return None


def _oracle_column_data_type(engine: Engine, schema: str | None, table_name: str, column_name: str) -> str:
    owner = (schema or "").upper()
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT data_type FROM all_tab_columns "
                    "WHERE owner = :owner AND table_name = :table AND column_name = :col"
                ),
                {"owner": owner, "table": table_name.upper(), "col": column_name.upper()},
            ).fetchone()
            return str(row[0]).upper() if row and row[0] else ""
    except Exception:
        return ""


def _oracle_column_type_details(
    engine: Engine,
    schema: str | None,
    table_name: str,
    column_name: str,
) -> dict[str, Any]:
    """Return Oracle dictionary metadata needed to validate a TDSQL partition key."""
    owner = (schema or "").upper()
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT data_type, data_precision, data_scale, data_length, char_length "
                    "FROM all_tab_columns WHERE owner = :owner "
                    "AND table_name = :table AND column_name = :col"
                ),
                {"owner": owner, "table": table_name.upper(), "col": column_name.upper()},
            ).fetchone()
        if not row:
            return {}
        return {
            "data_type": str(row[0] or "").upper(),
            "precision": int(row[1]) if row[1] is not None else None,
            "scale": int(row[2]) if row[2] is not None else None,
            "data_length": int(row[3]) if row[3] is not None else None,
            "char_length": int(row[4]) if row[4] is not None else None,
        }
    except Exception:
        return {}


def _normalize_oracle_type(data_type: str) -> str:
    return re.sub(r"\(.*\)", "", data_type or "").strip().upper()


def _oracle_primary_keys(engine: Engine, schema: str | None, table_name: str) -> list[str]:
    try:
        inspector = inspect(engine)
        return inspector.get_pk_constraint(table_name, schema=schema).get("constrained_columns") or []
    except Exception:
        return []


def _parse_high_value(high_value: str | None) -> str | None:
    """将 Oracle high_value 文本解析为 TDSQL 字面量，无法解析返回 None。

    - MAXVALUE 保留
    - 纯数字保留
    - TO_DATE('YYYY-MM-DD HH24:MI:SS', 'fmt'[, 'NLS_...']) 提取日期串并转 TDSQL 日期字面量
    - TIMESTAMP'...' / DATE'...' 字面量提取日期串
    - 字符串字面量保留单引号
    """
    if high_value is None:
        return None
    hv = high_value.strip()
    if not hv:
        return None
    if hv.upper() == "MAXVALUE":
        return "MAXVALUE"
    if re.fullmatch(r"[+-]?\d+(\.\d+)?", hv):
        return hv
    # TO_DATE('...', 'fmt') 或 TO_DATE('...', 'fmt', 'NLS_CALENDAR=...')
    to_date = re.search(
        r"TO_DATE\s*\(\s*'((?:[^']|'')*)'\s*(?:,\s*'[^']*')?\s*(?:,\s*'[^']*')?\s*\)",
        hv,
        re.IGNORECASE,
    )
    if to_date:
        literal = to_date.group(1).replace("''", "'").strip()
        return f"'{literal}'"
    # TIMESTAMP'...' / DATE'...' / TIMESTAMP '...'
    ts_literal = re.search(
        r"(?:TIMESTAMP|DATE)\s*'((?:[^']|'')*)'",
        hv,
        re.IGNORECASE,
    )
    if ts_literal:
        literal = ts_literal.group(1).replace("''", "'").strip()
        return f"'{literal}'"
    if hv.startswith("'") and hv.endswith("'"):
        return hv
    return None


def _split_comma_values(inner: str) -> list[str]:
    """按逗号切分值列表，引号内与括号内的逗号不切分（TO_DATE('...', 'fmt') 等表达式）。"""
    parts: list[str] = []
    cur: list[str] = []
    in_quote = False
    depth = 0
    for ch in inner:
        if ch == "'":
            in_quote = not in_quote
            cur.append(ch)
        elif ch == "(" and not in_quote:
            depth += 1
            cur.append(ch)
        elif ch == ")" and not in_quote:
            depth -= 1
            cur.append(ch)
        elif ch == "," and not in_quote and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return parts


def _parse_list_values(high_value: str | None) -> list[str]:
    """解析 Oracle LIST 分区 high_value 文本 '( a, b, c )' 为值列表。

    支持：纯数字、字符串字面量、NULL、MAXVALUE、TO_DATE 日期表达式。
    DEFAULT 分区返回 ["DEFAULT"]（由调用方决定降级策略）。
    """
    if not high_value:
        return []
    inner = high_value.strip()
    if inner.upper() == "DEFAULT":
        return ["DEFAULT"]
    if inner.startswith("(") and inner.endswith(")"):
        inner = inner[1:-1]
    values: list[str] = []
    for token in _split_comma_values(inner):
        token = token.strip()
        if not token:
            continue
        if token.upper() == "NULL":
            values.append("NULL")
        elif re.fullmatch(r"[+-]?\d+(\.\d+)?", token):
            values.append(token)
        elif token.startswith("'") and token.endswith("'"):
            values.append(token)
        elif token.upper() == "MAXVALUE":
            values.append("MAXVALUE")
        else:
            parsed = _parse_high_value(token)
            if parsed is None:
                # 无法识别的值，返回空标记整段不可用
                return []
            values.append(parsed)
    return values


def _partition_keys_missing_from_primary_key(
    part_info: dict[str, Any], primary_keys: list[str] | None
) -> list[str]:
    """Return partition keys that TDSQL requires but the source PK omits."""
    required_keys = [
        str(key) for key in (part_info.get("partition_key_columns") or [])
    ]
    if (part_info.get("subpartitioning_type") or "").upper() == "HASH":
        required_upper = {key.upper() for key in required_keys}
        for key in part_info.get("subpartition_key_columns") or []:
            value = str(key)
            if value.upper() not in required_upper:
                required_keys.append(value)
                required_upper.add(value.upper())
    pk_upper = {str(key).upper() for key in (primary_keys or [])}
    return [key for key in required_keys if key.upper() not in pk_upper]


def build_partition_clause(
    part_info: dict[str, Any],
    source_engine: Engine,
    source_schema: str | None,
    table_name: str,
    target_dialect: str,
    primary_keys: list[str] | None = None,
) -> tuple[str, list[str], list[str]]:
    """按 TDSQL 分区规则将 Oracle 分区信息转换为目标 CREATE TABLE 分区子句。

    返回 (partition_clause, warnings, partition_key_columns)。
    partition_clause 为空字符串表示降级为普通表（原因写入 warnings）。
    """
    warnings: list[str] = []
    ptype = part_info.get("partitioning_type") or ""
    sub_type = part_info.get("subpartitioning_type") or ""
    interval = (part_info.get("interval") or "NO").upper() == "YES"
    keys = [str(k) for k in (part_info.get("partition_key_columns") or [])]
    sub_keys = [str(k) for k in (part_info.get("subpartition_key_columns") or [])]
    partitions = part_info.get("partitions") or []
    pk = list(primary_keys or _oracle_primary_keys(source_engine, source_schema, table_name))

    if not ptype:
        return "", [f"未获取到表 {table_name} 的分区类型信息，已阻止创建普通表"], []
    if not keys:
        return "", [f"表 {table_name} 的分区键为表达式或无法解析，已阻止创建普通表"], []

    # 分区键必须包含在主键/唯一键中（TDSQL 硬约束）
    required_keys = list(keys)
    if sub_type == "HASH":
        required_keys.extend(
            key for key in sub_keys if key.upper() not in {k.upper() for k in required_keys}
        )
    missing = _partition_keys_missing_from_primary_key(part_info, pk)
    if not pk:
        return "", [f"源分区表 {table_name} 无主键；TDSQL 禁止无主键表，且不能擅自改变唯一性语义"], keys
    elif missing:
        return "", [f"表 {table_name} 的分区键 {', '.join(missing)} 不在主键中；为避免改变主键语义，已阻止转换"], keys

    # 分区键比普通字段受更多限制。特别是 TDSQL 的 RANGE/LIST/HASH
    # 表达式不能直接使用 DECIMAL/NUMERIC；只有可无损容纳到 BIGINT 的
    # Oracle 整数 NUMBER 才能转为整型分区键。
    key_details = {
        key: _oracle_column_type_details(
            source_engine, source_schema, table_name, key
        )
        for key in required_keys
    }
    for key, details in key_details.items():
        if not details:
            return "", [f"无法读取表 {table_name} 分区键 {key} 的字段精度，已阻止不确定转换"], keys
        data_type = _normalize_oracle_type(str(details.get("data_type") or ""))
        if data_type in {"NUMBER", "DECIMAL", "NUMERIC"}:
            precision = details.get("precision")
            scale = details.get("scale")
            if precision is None or (scale is not None and int(scale) > 0):
                return "", [f"表 {table_name} 分区键 {key} 为 {data_type}({precision},{scale})，不能无损转换为 TDSQL 整型分区键"], keys
            integer_digits = int(precision) - int(scale or 0)
            if integer_digits >= 19:
                return "", [f"表 {table_name} 分区键 {key} 需要 {integer_digits} 位整数，超出 TDSQL BIGINT 安全范围，已阻止转换"], keys
        elif data_type in {"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"}:
            return "", [f"表 {table_name} 分区键 {key} 为浮点类型 {data_type}，存在边界误差，已阻止转换"], keys
        elif data_type in {"CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2"}:
            if ptype == "HASH" or key.upper() in {k.upper() for k in sub_keys}:
                return "", [f"表 {table_name} 的 HASH 分区键 {key} 为字符类型，TDSQL HASH 不支持该键类型"], keys
            if int(details.get("char_length") or 0) > 255:
                warnings.append(f"表 {table_name} 分区键 {key} 长度超过 255，虽可建表但不符合 TDSQL 分区键建议")
        elif data_type not in {
            "DATE", "TIMESTAMP", "INTEGER", "INT", "SMALLINT", "BIGINT",
        }:
            return "", [f"表 {table_name} 分区键 {key} 的类型 {data_type} 不在 TDSQL 分区键白名单中"], keys

    def quote_key(value: str) -> str:
        return "`" + value.replace("`", "``") + "`"

    quoted_cols = ", ".join(quote_key(key) for key in keys)

    def range_partitions(column_mode: bool) -> tuple[str, str]:
        """生成 RANGE 分区定义。返回 (clause_body, error_or_empty)。"""
        if not partitions:
            return "", f"表 {table_name} 的 RANGE 分区无现有分区信息，降级为普通表"
        body_parts: list[str] = []
        has_maxvalue = False
        for idx, part in enumerate(partitions, start=1):
            hv = part.get("high_value")
            if column_mode:
                vals = _parse_list_values(hv) if hv else []
                if not vals:
                    vals = ["MAXVALUE"] * len(keys) if idx == len(partitions) else []
                if not vals:
                    return "", f"表 {table_name} 分区 {part.get('name')} 的 high_value 无法解析，降级为普通表"
                if any(v.upper() == "DEFAULT" for v in vals):
                    return "", f"表 {table_name} 分区 {part.get('name')} 含 DEFAULT 分区，TDSQL RANGE 不支持 DEFAULT，降级为普通表"
                if any(v.upper() == "MAXVALUE" for v in vals) and len(vals) < len(keys):
                    vals = vals + ["MAXVALUE"] * (len(keys) - len(vals))
                if any(v.upper() == "MAXVALUE" for v in vals):
                    has_maxvalue = True
                body_parts.append(f"PARTITION p{idx} VALUES LESS THAN ({', '.join(vals)})")
            else:
                val = _parse_high_value(hv)
                if val is None and idx < len(partitions):
                    return "", f"表 {table_name} 分区 {part.get('name')} 的 high_value 无法解析，降级为普通表"
                if val is None:
                    val = "MAXVALUE"
                if val.upper() == "MAXVALUE":
                    has_maxvalue = True
                body_parts.append(f"PARTITION p{idx} VALUES LESS THAN ({val})")
        # 间隔分区转普通 RANGE：若最后一个分区不含 MAXVALUE，补尾分区
        if interval and not has_maxvalue:
            body_parts.append(f"PARTITION p{len(partitions) + 1} VALUES LESS THAN (MAXVALUE)")
            warnings.append(f"间隔分区表 {table_name} 已转换为普通 RANGE 分区表（TDSQL 不支持 Oracle 间隔分区）")
        return "(" + ", ".join(body_parts) + ")", ""

    # 复合分区：主分区按 partitioning_type 生成，子分区按 subpartitioning_type 处理
    # 必须在单类型 RANGE/LIST/HASH 分支之前判断，否则会被单类型分支提前返回
    # Oracle 非复合分区 subpartitioning_type 为 "NONE"，需排除
    if ptype in {"RANGE", "LIST"} and sub_type and sub_type != "NONE":
        if ptype == "RANGE":
            single_col = len(keys) == 1
            col_type = _normalize_oracle_type(_oracle_column_data_type(source_engine, source_schema, table_name, keys[0]))
            numeric_int = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT", "NUMBER", "DECIMAL", "NUMERIC"}
            if single_col and col_type in numeric_int:
                body, err = range_partitions(column_mode=False)
                if err:
                    return "", [err], keys
                main_prefix = f"PARTITION BY RANGE ({quoted_cols})"
            else:
                body, err = range_partitions(column_mode=True)
                if err:
                    return "", [err], keys
                main_prefix = f"PARTITION BY RANGE COLUMNS ({quoted_cols})"
        else:
            if not partitions:
                return "", [f"表 {table_name} 的 LIST 分区无现有分区信息，降级为普通表"], keys
            body_parts = []
            for idx, part in enumerate(partitions, start=1):
                values = _parse_list_values(part.get("high_value"))
                if not values:
                    return "", [f"表 {table_name} 分区 {part.get('name')} 的 LIST high_value 无法解析，降级为普通表"], keys
                if any(v.upper() == "DEFAULT" for v in values):
                    return "", [f"表 {table_name} 分区 {part.get('name')} 为 DEFAULT 分区，TDSQL LIST 不支持 DEFAULT，降级为普通表（避免超范围数据无法插入）"], keys
                body_parts.append(f"PARTITION p{idx} VALUES IN ({', '.join(values)})")
            # 单列整型走 LIST，其余（日期/字符串/多列）走 LIST COLUMNS
            comp_single_col = len(keys) == 1
            comp_col_type = _normalize_oracle_type(_oracle_column_data_type(source_engine, source_schema, table_name, keys[0]))
            comp_numeric_int = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT", "NUMBER", "DECIMAL", "NUMERIC"}
            if comp_single_col and comp_col_type in comp_numeric_int:
                main_prefix = f"PARTITION BY LIST ({quoted_cols})"
            else:
                main_prefix = f"PARTITION BY LIST COLUMNS ({quoted_cols})"
            body = f"({', '.join(body_parts)})"

        if sub_type == "HASH" and sub_keys:
            sub_count = int(part_info.get("def_subpartition_count") or 0)
            if sub_count < 1 and part_info.get("subpartitions"):
                sub_count = len(part_info["subpartitions"]) // max(len(partitions), 1)
            if sub_count < 1:
                sub_count = 4
            sub_clause = f"SUBPARTITION BY HASH ({', '.join(quote_key(key) for key in sub_keys)}) SUBPARTITIONS {sub_count}"
            warnings.append(f"表 {table_name} 的复合分区 {ptype}-HASH 已保留子分区（HASH 子分区 N={sub_count}）")
            # TDSQL/MySQL 的语法顺序必须是主分区声明、子分区声明、分区明细。
            return f"{main_prefix} {sub_clause} {body}", warnings, required_keys
        warnings.append(f"表 {table_name} 的复合分区 {ptype}-{sub_type} 子分区已降级：仅保留主分区，不建子分区（TDSQL 仅支持 HASH/KEY 子分区）")
        return f"{main_prefix} {body}", warnings, keys

    if ptype == "RANGE":
        single_col = len(keys) == 1
        col_type = _normalize_oracle_type(_oracle_column_data_type(source_engine, source_schema, table_name, keys[0]))
        # TDSQL RANGE 支持整型键 + 整型边界；NUMERIC/DECIMAL 整数型在 DDL 渲染阶段映射为 BIGINT。
        # DATE/DATETIME/TIMESTAMP 键的 RANGE 边界 TDSQL 要求 INT（实测 1697），需走 RANGE COLUMNS。
        numeric_int = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT", "NUMBER", "DECIMAL", "NUMERIC"}
        if single_col and col_type in numeric_int:
            body, err = range_partitions(column_mode=False)
            if err:
                return "", [err], keys
            return f"PARTITION BY RANGE ({quoted_cols}) {body}", warnings, keys
        body, err = range_partitions(column_mode=True)
        if err:
            return "", [err], keys
        return f"PARTITION BY RANGE COLUMNS ({quoted_cols}) {body}", warnings, keys

    if ptype == "LIST":
        single_col = len(keys) == 1
        col_type = _normalize_oracle_type(_oracle_column_data_type(source_engine, source_schema, table_name, keys[0]))
        # TDSQL LIST 单列仅整型走 LIST；DATE/DATETIME/TIMESTAMP/字符串/多列走 LIST COLUMNS（实测边界值支持一致）
        numeric_int = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT", "NUMBER", "DECIMAL", "NUMERIC"}
        if not partitions:
            return "", [f"表 {table_name} 的 LIST 分区无现有分区信息，降级为普通表"], keys
        body_parts: list[str] = []
        for idx, part in enumerate(partitions, start=1):
            values = _parse_list_values(part.get("high_value"))
            if not values:
                return "", [f"表 {table_name} 分区 {part.get('name')} 的 LIST high_value 无法解析，降级为普通表"], keys
            if any(v.upper() == "DEFAULT" for v in values):
                return "", [f"表 {table_name} 分区 {part.get('name')} 为 DEFAULT 分区，TDSQL LIST 不支持 DEFAULT，降级为普通表（避免超范围数据无法插入）"], keys
            body_parts.append(f"PARTITION p{idx} VALUES IN ({', '.join(values)})")
        body = "(" + ", ".join(body_parts) + ")"
        if single_col and col_type in numeric_int:
            return f"PARTITION BY LIST ({quoted_cols}) {body}", warnings, keys
        return f"PARTITION BY LIST COLUMNS ({quoted_cols}) {body}", warnings, keys

    if ptype == "HASH":
        count = int(part_info.get("partition_count") or len(partitions) or 1)
        if count < 1:
            count = 1
        return f"PARTITION BY HASH ({quoted_cols}) PARTITIONS {count}", warnings, keys

    return "", [f"表 {table_name} 的分区类型 {ptype} 不受支持，降级为普通表"], keys


def _render_partitioned_create_ddl(
    target_table: Table,
    target_engine: Engine,
    partition_clause: str,
    primary_key_columns: list[str] | None = None,
    skip_columns: set[str] | None = None,
    partition_key_columns: list[str] | None = None,
) -> str:
    """手工构造带分区子句的 CREATE TABLE DDL 文本。

    分区键列若为 NUMERIC/DECIMAL 整数型，映射为 BIGINT（TDSQL 分区键不支持
    NUMERIC/DECIMAL，实测错误 1659）。
    """
    dialect = target_engine.dialect
    prep = dialect.identifier_preparer
    skip = skip_columns or set()
    pk = [c for c in (primary_key_columns or []) if c not in skip]
    pk_upper = {c.upper() for c in pk}
    part_key_upper = {c.upper() for c in (partition_key_columns or [])}
    col_defs: list[str] = []
    for col in target_table.columns:
        if col.name.upper() in skip:
            continue
        type_str = col.type.compile(dialect=dialect)
        if col.name.upper() in part_key_upper:
            base = type_str.upper()
            if re.match(r"NUMERIC|DECIMAL|NUMBER", base):
                match = re.search(r"\(\s*(\d+)\s*,\s*(-?\d+)", base)
                precision = int(match.group(1)) if match else None
                scale = int(match.group(2)) if match else None
                integer_digits = precision - (scale or 0) if precision is not None else None
                if scale is None or scale > 0 or integer_digits is None or integer_digits >= 19:
                    raise RuntimeError(
                        f"分区键 {col.name} 的类型 {type_str} 不能无损转换为 TDSQL BIGINT"
                    )
                type_str = "BIGINT"
        parts = [prep.quote(col.name), type_str]
        if not col.nullable:
            parts.append("NOT NULL")
        if col.name.upper() in pk_upper and col.autoincrement is True:
            parts.append("AUTO_INCREMENT")
        col_defs.append(" ".join(parts))
    if pk:
        col_defs.append("PRIMARY KEY (" + ", ".join(prep.quote(c) for c in pk) + ")")
    qualified = f"{prep.quote_schema(target_table.schema)}.{prep.quote(target_table.name)}" if target_table.schema else prep.quote(target_table.name)
    ddl = (
        f"CREATE TABLE {qualified} (\n  "
        + ",\n  ".join(col_defs)
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )
    if partition_clause:
        ddl += " " + partition_clause
    return ddl


def estimate_row_size(row: Iterable[Any]) -> int:
    total = 0
    for value in row:
        if value is None:
            continue
        if isinstance(value, bytes):
            total += len(value)
        elif isinstance(value, (str, Decimal)):
            total += len(str(value).encode("utf-8"))
        else:
            total += 8
    return total


@dataclass
class PreparedTable:
    source: Table
    target: Table
    column_names: list[str]
    partition_warnings: list[str] = field(default_factory=list)
    partition_info: dict[str, Any] | None = None
    partition_clause: str = ""


def _mysql_table_is_partitioned(
    engine: Engine, schema: str | None, table_name: str
) -> bool:
    """Verify the physical target shape instead of trusting the task label."""
    try:
        with engine.connect() as connection:
            count = connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.PARTITIONS "
                    "WHERE TABLE_SCHEMA = COALESCE(:schema, DATABASE()) "
                    "AND TABLE_NAME = :table AND PARTITION_NAME IS NOT NULL"
                ),
                {"schema": schema, "table": table_name},
            ).scalar()
        return int(count or 0) > 0
    except Exception:
        return False


def prepare_table(source_engine: Engine, target_engine: Engine, source_schema: str | None, target_schema: str | None, name: str, mode: str, create_tables: bool, target_name: str | None = None) -> PreparedTable:
    partition_info = None
    partition_warnings: list[str] = []
    partition_clause = ""
    target_dialect = target_engine.dialect.name
    is_partitioned = False
    if source_engine.dialect.name == "oracle" and target_dialect in {"mysql", "tdsql"}:
        partition_info = get_oracle_partition_info(source_engine, source_schema, name)
        is_partitioned = partition_info is not None
    actual_target_name = target_name or name
    target_table, column_names = build_target_table(source_engine, target_engine, source_schema, target_schema, name, actual_target_name)
    source_table = reflect_source_object(source_engine, source_schema, name)
    exists = inspect(target_engine).has_table(actual_target_name, schema=target_schema)
    quoted = target_engine.dialect.identifier_preparer
    qualified = f"{quoted.quote_schema(target_schema)}.{quoted.quote(actual_target_name)}" if target_schema else quoted.quote(actual_target_name)
    if exists and mode == "fail":
        raise RuntimeError(f"目标表已存在：{actual_target_name}")
    if (
        exists
        and is_partitioned
        and create_tables
        and mode != "drop_and_create"
        and not _mysql_table_is_partitioned(target_engine, target_schema, actual_target_name)
    ):
        raise RuntimeError(
            f"源表 {name} 是分区表，但目标端现有同名表是普通表；"
            "请选择删除并重建，或先人工创建兼容的目标分区表"
        )
    if exists and mode == "drop_and_create":
        target_table.drop(target_engine, checkfirst=True)
        exists = False
    if exists and mode == "truncate":
        with target_engine.begin() as connection:
            connection.execute(text(f"TRUNCATE TABLE {qualified}"))
    if not exists:
        if not create_tables:
            raise RuntimeError(f"目标表不存在：{actual_target_name}")
        if is_partitioned and partition_info:
            primary_keys = _oracle_primary_keys(source_engine, source_schema, name)
            partition_clause, partition_warnings, partition_keys = build_partition_clause(
                partition_info,
                source_engine,
                source_schema,
                name,
                target_dialect,
                primary_keys=primary_keys,
            )
            if partition_clause:
                # 分区表：手工构造 CREATE TABLE DDL（列定义 + 主键 + 分区子句）
                ddl = _render_partitioned_create_ddl(
                    target_table, target_engine, partition_clause,
                    primary_key_columns=primary_keys,
                    partition_key_columns=partition_keys,
                )
                with target_engine.begin() as connection:
                    connection.execute(text(ddl))
            elif primary_keys and (
                missing_keys := _partition_keys_missing_from_primary_key(
                    partition_info, primary_keys
                )
            ):
                # Oracle permits a global PK that omits the partition key, while
                # TDSQL/MySQL partitioned tables require every unique key to
                # contain it. Expanding the PK would weaken the original ID
                # uniqueness. Preserve the source PK and data by creating a
                # normal target table, and make the physical downgrade explicit.
                target_table.create(target_engine)
                partition_warnings = [
                    f"源表 {name} 为分区表，但分区键 {', '.join(missing_keys)} "
                    f"不在源主键 ({', '.join(primary_keys)}) 中；TDSQL 无法同时保留该分区结构"
                    "与原主键唯一性，目标端已安全降级为普通表并保留原主键，数据将完整迁移"
                ]
            else:
                # 其他无法证明安全的转换问题仍然阻止迁移，避免静默改变
                # 分区边界、字段精度或唯一性语义。
                reason = "；".join(partition_warnings) or "未知分区转换错误"
                raise RuntimeError(f"分区表 {name} 转换失败：{reason}")
        else:
            target_table.create(target_engine)
    return PreparedTable(
        source_table, target_table, column_names,
        partition_warnings=partition_warnings,
        partition_info=partition_info,
        partition_clause=partition_clause,
    )


def selectable_columns(source_engine: Engine, target_engine: Engine, source_table: Table):
    selected_columns = []
    for column in source_table.columns:
        source_type_name = column.type.__class__.__name__.upper()
        if (
            source_engine.dialect.name == "oracle"
            and target_engine.dialect.name == "mysql"
            and "TIMESTAMP" in source_type_name
            and bool(getattr(column.type, "timezone", False))
        ):
            selected_columns.append(func.to_char(column, "YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM").label(column.name))
        elif "BFILE" in source_type_name or "BFILE" in str(column.type).upper():
            # BFILE 是外部文件引用（DIRECTORY + 文件名），数据不在库内；读取 LOB
            # 会触发源端 FILEOPEN 打开外部 OS 文件，文件缺失即报 ORA-22288。
            # 迁移时该列置 NULL，外部文件内容不随库迁移。
            selected_columns.append(null().label(column.name))
        else:
            selected_columns.append(column)
    return selected_columns


def copy_batches(source_engine: Engine, target_engine: Engine, prepared: PreparedTable, batch_size: int):
    selected_columns = selectable_columns(source_engine, target_engine, prepared.source)
    with source_engine.connect().execution_options(stream_results=True) as source_connection:
        result = source_connection.execute(select(*selected_columns)).mappings()
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            target_types = {column.name: column.type for column in prepared.target.columns}
            payload = [
                {name: adapt_value(row[name], target_types[name]) for name in prepared.column_names}
                for row in rows
            ]
            with target_engine.begin() as target_connection:
                target_connection.execute(prepared.target.insert(), payload)
            yield len(payload), sum(estimate_row_size(row.values()) for row in payload)


def format_interval_ym(value: Any) -> str:
    """Format an oracledb.IntervalYM value in the Oracle canonical form.

    INTERVAL YEAR TO MONTH is displayed by Oracle as e.g. +0026-08
    (sign, 4-digit year, dash, 2-digit month). oracledb returns an
    IntervalYM object with years/months attributes.
    """
    total_months = value.years * 12 + value.months
    sign = "-" if total_months < 0 else "+"
    total_months = abs(total_months)
    years, months = divmod(total_months, 12)
    return f"{sign}{years:04d}-{months:02d}"


def adapt_value(value: Any, target_type: Any) -> Any:
    if value is None:
        return None
    if isinstance(target_type, String) and isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(target_type, String) and isinstance(value, timedelta):
        # Oracle INTERVAL DAY TO SECOND arrives as timedelta. Stringify with
        # the canonical Python form so validation compares equal.
        return str(value)
    if isinstance(target_type, String) and type(value).__name__ == "IntervalYM":
        # Oracle INTERVAL YEAR TO MONTH arrives as oracledb.IntervalYM.
        # Format in the Oracle canonical form (+0026-08) so the target
        # string column stores the true value and validation compares equal.
        return format_interval_ym(value)
    if hasattr(value, "read"):
        return value.read()
    return value
