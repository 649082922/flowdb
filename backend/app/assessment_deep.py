"""DBA-level deep pre-migration assessment.

Collects instance/environment facts, key parameter comparison, object scale
statistics, data volume analysis, and foreign-key dependency summaries from
Oracle / MySQL / PostgreSQL source and target databases.

Every metadata query is defensive: failures degrade to null and are recorded
in ``notes`` instead of crashing the whole assessment.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, String, Table, func, inspect, select, text

from .database import default_schema, make_engine, portable_type_info
from .models import ConnectionConfig

SYSTEM_SCHEMAS_SQL = "('pg_catalog','information_schema')"

# ---------------------------------------------------------------------------
# small query helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _fetch(engine, sql, notes, section, params=None, single_row: bool = False):
    """Run a query; on failure record a note and return None."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            if single_row:
                row = result.mappings().first()
                return dict(row) if row else None
            rows = result.mappings().all()
            return [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001 - defensive degradation is intended
        notes.append({"section": section, "message": f"{type(exc).__name__}: {str(exc)[:300]}"})
        return None


def _fetch_one(engine, sql, notes, section, params=None):
    return _fetch(engine, sql, notes, section, params, single_row=True)


def _in_clause(values: tuple | list) -> str:
    """Render a safe IN clause literal from a fixed code-level whitelist.

    Values come from hard-coded constants (never user input), so inlining
    avoids driver-specific array binding (e.g. Oracle ORA-01484).
    """
    return "(" + ",".join(repr(str(v)) for v in values) + ")"


def _fetch_scalar(engine, sql, notes, section, params=None):
    row = _fetch_one(engine, sql, notes, section, params)
    if row is None:
        return None
    for value in row.values():
        return value
    return None


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any, limit: int = 4000) -> str | None:
    if value is None:
        return None
    text_value = str(value)
    return text_value if len(text_value) <= limit else text_value[:limit]


DETAIL_LIST_LIMIT = 100


def _truncated_detail(rows: list | None, total: int | None) -> dict | None:
    """Wrap a detail list with truncation info. rows=None means query failed."""
    if rows is None:
        return None
    truncated = len(rows) > DETAIL_LIST_LIMIT
    return {
        "items": rows[:DETAIL_LIST_LIMIT],
        "truncated": truncated,
        "total": int(total) if total is not None else None,
    }


def _dialect_compile(name: str):
    """Return an SQLAlchemy dialect for compiling type DDL strings."""
    if name == "mysql":
        from sqlalchemy.dialects import mysql as _mysql
        return _mysql.dialect()
    if name == "postgresql":
        from sqlalchemy.dialects import postgresql as _pg
        return _pg.dialect()
    from sqlalchemy.dialects import oracle as _oracle
    return _oracle.dialect()


# ---------------------------------------------------------------------------
# Oracle collector
# ---------------------------------------------------------------------------

ORACLE_V_PARAMETERS = (
    "sga_target",
    "sga_max_size",
    "pga_aggregate_target",
    "db_block_size",
    "processes",
    "sessions",
    "open_cursors",
    "compatible",
    "db_files",
    "undo_tablespace",
    "log_buffer",
    "cpu_count",
    "nls_length_semantics",
    "serializable",
    "parallel_max_servers",
)

ORACLE_NLS_PARAMETERS = (
    "NLS_CHARACTERSET",
    "NLS_NCHAR_CHARACTERSET",
    "NLS_COMP",
    "NLS_SORT",
    "NLS_LANGUAGE",
    "NLS_TERRITORY",
    "NLS_DATE_FORMAT",
    "NLS_TIMESTAMP_FORMAT",
    "NLS_TIMEZONE",
    "NLS_LENGTH_SEMANTICS",
)

ORACLE_OSSTAT = ("NUM_CPUS", "PHYSICAL_MEMORY_BYTES", "LOAD", "GLOBAL_MEMORY_BOUND")

ORACLE_OBJECT_COUNTS = (
    ("tables", "tables", "SELECT COUNT(*) AS c FROM {p}_tables"),
    ("views", "views", "SELECT COUNT(*) AS c FROM {p}_views"),
    ("sequences", "sequences", "SELECT COUNT(*) AS c FROM {p}_sequences"),
    ("synonyms", "synonyms", "SELECT COUNT(*) AS c FROM {p}_synonyms"),
    ("dblinks", "dblinks", "SELECT COUNT(*) AS c FROM {p}_db_links"),
    ("procedures", "procedures", "SELECT COUNT(*) AS c FROM {p}_procedures WHERE object_type='PROCEDURE'"),
    ("functions", "functions", "SELECT COUNT(*) AS c FROM {p}_procedures WHERE object_type='FUNCTION'"),
    ("packages", "packages", "SELECT COUNT(*) AS c FROM {p}_objects WHERE object_type IN ('PACKAGE','PACKAGE BODY')"),
    ("triggers", "triggers", "SELECT COUNT(*) AS c FROM {p}_triggers"),
    ("materialized_views", "materialized_views", "SELECT COUNT(*) AS c FROM {p}_mviews"),
    ("indexes", "indexes", "SELECT COUNT(*) AS c FROM {p}_indexes"),
    ("constraints", "constraints", "SELECT COUNT(*) AS c FROM {p}_constraints"),
    ("partitioned_tables", "partitioned_tables", "SELECT COUNT(*) AS c FROM {p}_tables WHERE partitioned='YES'"),
    ("scheduler_jobs", "scheduler_jobs", "SELECT COUNT(*) AS c FROM {p}_scheduler_jobs"),
)

# 按 owner/schema 收敛的计数 SQL（仅 dba 前缀可用）；:owner 使用参数绑定
ORACLE_OBJECT_COUNTS_OWNER = {
    "tables": "SELECT COUNT(*) AS c FROM {p}_tables WHERE owner = :owner",
    "views": "SELECT COUNT(*) AS c FROM {p}_views WHERE owner = :owner",
    "sequences": "SELECT COUNT(*) AS c FROM {p}_sequences WHERE sequence_owner = :owner",
    "synonyms": "SELECT COUNT(*) AS c FROM {p}_synonyms WHERE owner = :owner",
    "dblinks": "SELECT COUNT(*) AS c FROM {p}_db_links WHERE owner = :owner",
    "procedures": "SELECT COUNT(*) AS c FROM {p}_procedures WHERE object_type='PROCEDURE' AND owner = :owner",
    "functions": "SELECT COUNT(*) AS c FROM {p}_procedures WHERE object_type='FUNCTION' AND owner = :owner",
    "packages": "SELECT COUNT(*) AS c FROM {p}_objects WHERE object_type IN ('PACKAGE','PACKAGE BODY') AND owner = :owner",
    "triggers": "SELECT COUNT(*) AS c FROM {p}_triggers WHERE owner = :owner",
    "materialized_views": "SELECT COUNT(*) AS c FROM {p}_mviews WHERE owner = :owner",
    "indexes": "SELECT COUNT(*) AS c FROM {p}_indexes WHERE owner = :owner",
    "constraints": "SELECT COUNT(*) AS c FROM {p}_constraints WHERE owner = :owner",
    "partitioned_tables": "SELECT COUNT(*) AS c FROM {p}_tables WHERE partitioned='YES' AND owner = :owner",
    "scheduler_jobs": "SELECT COUNT(*) AS c FROM {p}_scheduler_jobs WHERE owner = :owner",
}


def _oracle_scope(engine, notes) -> str:
    """Detect whether DBA dictionary views are readable; fall back to user_*."""
    probe = _fetch_one(engine, "SELECT COUNT(*) AS c FROM dba_tables", notes, "oracle.object.scope")
    if probe is not None:
        return "dba"
    notes.append({
        "section": "oracle.object.scope",
        "message": "无法读取 DBA 数据字典（无 dba_tables 权限），对象统计降级为当前用户(user_*)视角",
    })
    return "user"


def _oracle_owner_column(prefix: str) -> str:
    return "owner" if prefix == "dba" else ""


def _apply_owner_condition(sql: str, owners: list[str] | None, params: dict[str, Any]) -> str:
    """将 SQL 模板中的 ``= :owner`` 绑定展开为单 owner 或 IN 列表，并填充 params。

    兼容 ``owner = :owner`` / ``sequence_owner = :owner`` / ``AND owner = :owner`` 等写法；
    owners 为空时原样返回（调用方负责传空 params 或保持 USER 语义）。
    """
    if not owners:
        return sql
    if len(owners) == 1:
        params["owner"] = owners[0]
        return sql
    placeholders = ", ".join(f":owner_{i}" for i in range(len(owners)))
    for i, item in enumerate(owners):
        params[f"owner_{i}"] = item
    return sql.replace("= :owner", f"IN ({placeholders})")


def _collect_oracle(engine, config: ConnectionConfig, notes: list[dict]) -> dict[str, Any]:
    env: dict[str, Any] = {"dialect": "oracle", "notes": []}
    instance = _fetch_one(
        engine,
        "SELECT instance_name, host_name, version, status, parallel, startup_time, database_status FROM v$instance",
        notes, "oracle.instance",
    )
    if instance:
        env["version"] = _safe_str(instance.get("version"))
        env["host"] = _safe_str(instance.get("host_name"))
        env["database"] = _safe_str(instance.get("instance_name"))
        env["startup_time"] = _iso(instance.get("startup_time"))
        env["status"] = _safe_str(instance.get("status"))
        env["run_mode"] = "RAC（parallel 实例）" if str(instance.get("parallel") or "").upper() == "YES" else "单实例"
    database = _fetch_one(
        engine,
        "SELECT name, created, log_mode, open_mode, platform_name, dbid FROM v$database",
        notes, "oracle.database",
    )
    if database:
        env.setdefault("database", _safe_str(database.get("name")))
        env["created"] = _iso(database.get("created"))
        env["log_mode"] = _safe_str(database.get("log_mode"))
        env["open_mode"] = _safe_str(database.get("open_mode"))
        env["platform_name"] = _safe_str(database.get("platform_name"))
    env["parameters"] = {}
    rows = _fetch(
        engine,
        f"SELECT name, value, display_value FROM v$parameter WHERE name IN {_in_clause(ORACLE_V_PARAMETERS)}",
        notes, "oracle.parameters",
    )
    if rows is not None:
        for row in rows:
            name = str(row.get("name") or "").upper()
            env["parameters"][name] = _safe_str(row.get("display_value") if row.get("display_value") is not None else row.get("value"))
    nls = _fetch(
        engine,
        f"SELECT parameter, value FROM nls_database_parameters WHERE parameter IN {_in_clause(ORACLE_NLS_PARAMETERS)}",
        notes, "oracle.nls",
    )
    if nls is not None:
        for row in nls:
            env["parameters"][str(row.get("parameter")).upper()] = _safe_str(row.get("value"))
    env["charset"] = env["parameters"].get("NLS_CHARACTERSET")
    # host resources
    host_resources = {}
    osstat = _fetch(
        engine,
        f"SELECT stat_name, value FROM v$osstat WHERE stat_name IN {_in_clause(ORACLE_OSSTAT)}",
        notes, "oracle.osstat",
    )
    if osstat is None:
        env["host_resources"] = None
        notes.append({"section": "oracle.host_resources", "message": "无法读取 v$osstat（无权限），主机资源信息缺失"})
    else:
        for row in osstat:
            key = str(row.get("stat_name") or "")
            value = _int(row.get("value"))
            if key == "NUM_CPUS" and value:
                host_resources["cpu_cores"] = value
            elif key == "PHYSICAL_MEMORY_BYTES" and value:
                host_resources["memory_bytes"] = value
        env["host_resources"] = host_resources if host_resources else None
    return env


def _collect_oracle_objects(engine, notes: list[dict], owners: list[str] | None = None) -> dict[str, Any]:
    prefix = _oracle_scope(engine, notes)
    result: dict[str, Any] = {"scope": "dba" if prefix == "dba" else "user", "counts": {}}
    if owners:
        result["owners"] = owners
    for key, _label, sql_template in ORACLE_OBJECT_COUNTS:
        if prefix == "dba" and owners:
            params: dict[str, Any] = {}
            sql = _apply_owner_condition(ORACLE_OBJECT_COUNTS_OWNER.get(key, sql_template).format(p=prefix), owners, params)
            count = _fetch_scalar(engine, sql, notes, f"oracle.objects.{key}", params=params)
        else:
            sql = sql_template.format(p=prefix)
            count = _fetch_scalar(engine, sql, notes, f"oracle.objects.{key}")
        if count is None:
            result["counts"][key] = None
        else:
            result["counts"][key] = int(count)
    # current user note for dba scope
    if prefix == "dba":
        user_row = _fetch_one(engine, "SELECT USER AS current_user FROM dual", notes, "oracle.current_user")
        if user_row and user_row.get("current_user"):
            result["current_user"] = str(user_row["current_user"])
    result["details"] = _collect_oracle_details(engine, notes, owners=owners)
    return result


def _collect_oracle_partitions(engine, notes: list[dict], owners: list[str] | None = None) -> dict[str, Any]:
    """P2: 分区表分析（类型分布 + 间隔分区清单）。

    查 all_part_tables 统计 partitioning_type 分布与 interval='YES' 的间隔分区表，
    供评估风险提示与迁移降级预判使用。查询失败整体降级为 null 并记录 notes。
    """
    prefix = _oracle_scope(engine, notes)
    result: dict[str, Any] = {"partitioned_total": 0, "interval_tables": [], "by_type": {}, "downgrades": []}

    # 类型分布（含子分区类型，复合分区以 pt-st 表示）
    sql = f"SELECT partitioning_type, subpartitioning_type, COUNT(*) AS c FROM {prefix}_part_tables"
    params: dict[str, Any] = {}
    if prefix == "dba" and owners:
        placeholders = ", ".join(f":owner_{i}" for i in range(len(owners)))
        sql += f" WHERE owner IN ({placeholders})"
        for i, item in enumerate(owners):
            params[f"owner_{i}"] = item
    sql += " GROUP BY partitioning_type, subpartitioning_type"
    rows = _fetch(engine, sql, notes, "oracle.partition.types", params=params)
    total = 0
    if rows is not None:
        for row in rows:
            pt = str(row.get("partitioning_type") or "").upper() or "其他"
            st = str(row.get("subpartitioning_type") or "").upper()
            label = f"{pt}-{st}" if st else pt
            result["by_type"][label] = result["by_type"].get(label, 0) + int(row.get("c") or 0)
            total += int(row.get("c") or 0)
    result["partitioned_total"] = total

    # 间隔分区表清单：Oracle 该版本 all_part_tables.interval 对间隔分区存间隔定义文本
    # （如 NUMTOYMINTERVAL(1,'MONTH')），非间隔为 NULL，不能用 interval = 'YES' 过滤
    sql2 = f"SELECT table_name FROM {prefix}_part_tables WHERE interval IS NOT NULL"
    params2: dict[str, Any] = {}
    if prefix == "dba" and owners:
        placeholders = ", ".join(f":owner_{i}" for i in range(len(owners)))
        sql2 += f" AND owner IN ({placeholders})"
        for i, item in enumerate(owners):
            params2[f"owner_{i}"] = item
    sql2 += " ORDER BY table_name"
    it_rows = _fetch(engine, sql2, notes, "oracle.partition.interval", params=params2)
    names: list[str] = []
    if it_rows is not None:
        for row in it_rows:
            names.append(str(row.get("table_name") or ""))
    result["interval_tables"] = names
    if names:
        shown = "、".join(names[:10]) + ("…" if len(names) > 10 else "")
        result["downgrades"].append(
            f"检测到 {len(names)} 个 Oracle 间隔分区表（{shown}），TDSQL 不支持间隔分区，迁移时将转换为普通 RANGE 分区表"
        )
    return result


def _collect_oracle_details(engine, notes: list[dict], owners: list[str] | None = None) -> dict[str, Any]:
    """P1: object detail lists (sequences / synonyms / dblinks / routines / triggers)."""
    prefix = _oracle_scope(engine, notes)
    if prefix == "dba" and owners:
        params: dict[str, Any] = {}
        owner_filter = _apply_owner_condition("WHERE owner = :owner", owners, params)
        seq_owner_filter = _apply_owner_condition("WHERE sequence_owner = :owner", owners, params)
        owner_and = _apply_owner_condition("AND owner = :owner", owners, params)
    else:
        owner_filter = "WHERE owner = USER" if prefix == "dba" else ""
        seq_owner_filter = "WHERE sequence_owner = USER" if prefix == "dba" else ""
        owner_and = "AND owner = USER" if prefix == "dba" else ""
        params = {}
    result: dict[str, Any] = {"scope": "dba" if prefix == "dba" else "user"}
    if owners:
        result["owners"] = owners

    seq_rows = _fetch(engine, f"""
        SELECT sequence_name, last_number, increment_by, cache_size, cycle_flag
        FROM {prefix}_sequences {seq_owner_filter}
        ORDER BY sequence_name FETCH FIRST {DETAIL_LIST_LIMIT + 1} ROWS ONLY""",
        notes, "oracle.details.sequences", params=params)
    seq_count = _fetch_scalar(engine, f"SELECT COUNT(*) AS c FROM {prefix}_sequences {seq_owner_filter}",
                              notes, "oracle.details.sequences.count", params=params)
    result["sequences"] = _truncated_detail(seq_rows, seq_count)

    syn_rows = _fetch(engine, f"""
        SELECT synonym_name, table_owner, table_name, db_link
        FROM {prefix}_synonyms {owner_filter}
        ORDER BY synonym_name FETCH FIRST {DETAIL_LIST_LIMIT + 1} ROWS ONLY""",
        notes, "oracle.details.synonyms", params=params)
    syn_count = _fetch_scalar(engine, f"SELECT COUNT(*) AS c FROM {prefix}_synonyms {owner_filter}",
                              notes, "oracle.details.synonyms.count", params=params)
    result["synonyms"] = _truncated_detail(syn_rows, syn_count)

    link_rows = _fetch(engine, f"""
        SELECT db_link, username, host
        FROM {prefix}_db_links {owner_filter}
        ORDER BY db_link FETCH FIRST {DETAIL_LIST_LIMIT + 1} ROWS ONLY""",
        notes, "oracle.details.dblinks", params=params)
    link_count = _fetch_scalar(engine, f"SELECT COUNT(*) AS c FROM {prefix}_db_links {owner_filter}",
                               notes, "oracle.details.dblinks.count", params=params)
    result["dblinks"] = _truncated_detail(link_rows, link_count)

    proc_rows = _fetch(engine, f"""
        SELECT object_name, object_type, status
        FROM {prefix}_objects
        WHERE object_type IN ('PROCEDURE','FUNCTION') {owner_and}
        ORDER BY object_type, object_name FETCH FIRST {DETAIL_LIST_LIMIT + 1} ROWS ONLY""",
        notes, "oracle.details.routines", params=params)
    proc_count = _fetch_scalar(engine, f"""
        SELECT COUNT(*) AS c FROM {prefix}_objects
        WHERE object_type IN ('PROCEDURE','FUNCTION') {owner_and}""",
        notes, "oracle.details.routines.count", params=params)
    result["procedures"] = _truncated_detail(proc_rows, proc_count)
    if proc_rows is not None:
        invalid = [r for r in proc_rows if str(r.get("status") or "").upper() == "INVALID"]
        if invalid:
            notes.append({"section": "oracle.details.routines", "message": f"检测到 {len(invalid)} 个 INVALID 存储过程/函数（如 {'、'.join(str(r['object_name']) for r in invalid[:3])}）"})

    trig_rows = _fetch(engine, f"""
        SELECT trigger_name, table_name, status, trigger_type, triggering_event
        FROM {prefix}_triggers {owner_filter}
        ORDER BY trigger_name FETCH FIRST {DETAIL_LIST_LIMIT + 1} ROWS ONLY""",
        notes, "oracle.details.triggers", params=params)
    trig_count = _fetch_scalar(engine, f"SELECT COUNT(*) AS c FROM {prefix}_triggers {owner_filter}",
                               notes, "oracle.details.triggers.count", params=params)
    result["triggers"] = _truncated_detail(trig_rows, trig_count)
    if trig_rows is not None:
        disabled = [r for r in trig_rows if str(r.get("status") or "").upper() == "DISABLED"]
        if disabled:
            notes.append({"section": "oracle.details.triggers", "message": f"检测到 {len(disabled)} 个 DISABLED 触发器（如 {'、'.join(str(r['trigger_name']) for r in disabled[:3])}）"})
    return result


def _collect_oracle_data(engine, notes: list[dict], owners: list[str] | None = None) -> dict[str, Any]:
    prefix = _oracle_scope(engine, notes)
    owner_filter = ""
    owner_join = ""
    owner_table = ""
    params: dict[str, Any] = {}
    if prefix == "dba" and owners:
        owner_filter = _apply_owner_condition("WHERE t.owner = :owner", owners, params)
        owner_table = "t.owner || '.' || t.table_name AS qualified_name,"
    elif prefix == "dba":
        owner_filter = "WHERE t.owner = USER"
        owner_table = "t.owner || '.' || t.table_name AS qualified_name,"
    result: dict[str, Any] = {"scope": "dba" if prefix == "dba" else "user"}
    if owners:
        result["owners"] = owners

    total_bytes_params = dict(params) if (prefix == "dba" and owners) else {}
    total_bytes_sql = (
        _apply_owner_condition("SELECT NVL(SUM(bytes),0) AS c FROM {p}_segments WHERE owner = :owner".format(p=prefix), owners, total_bytes_params)
        if prefix == "dba" and owners else
        "SELECT NVL(SUM(bytes),0) AS c FROM {p}_segments".format(p=prefix)
    )
    total_bytes = _fetch_scalar(engine, total_bytes_sql, notes, "oracle.data.total_bytes", params=total_bytes_params or None)
    result["total_bytes"] = int(total_bytes) if total_bytes is not None else None
    total_rows_sql = (
        _apply_owner_condition("SELECT NVL(SUM(num_rows),0) AS c FROM {p}_tables WHERE owner = :owner".format(p=prefix), owners, total_bytes_params)
        if prefix == "dba" and owners else
        "SELECT NVL(SUM(num_rows),0) AS c FROM {p}_tables".format(p=prefix)
    )
    total_rows = _fetch_scalar(engine, total_rows_sql, notes, "oracle.data.total_rows", params=total_bytes_params or None)
    result["total_rows_estimate"] = int(total_rows) if total_rows is not None else None
    # top 10 tables by segment bytes
    top_sql = f"""
        SELECT * FROM (
          SELECT {owner_table}
                 t.table_name,
                 NVL(s.bytes,0) AS size_bytes,
                 NVL(t.num_rows,0) AS rows_estimate,
                 t.partitioned,
                 (SELECT COUNT(*) FROM {prefix}_tab_columns c
                   WHERE c.table_name = t.table_name
                   {("AND c.owner = t.owner" if prefix == "dba" else "")}) AS column_count,
                 CASE WHEN EXISTS (
                   SELECT 1 FROM {prefix}_constraints k
                   WHERE k.table_name = t.table_name
                     AND k.constraint_type = 'P'
                     {("AND k.owner = t.owner" if prefix == "dba" else "")}
                 ) THEN 1 ELSE 0 END AS has_pk
          FROM {prefix}_tables t
          LEFT JOIN (
            SELECT segment_name, SUM(bytes) AS bytes
            FROM {prefix}_segments
            WHERE segment_type IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION')
            GROUP BY segment_name
          ) s ON s.segment_name = t.table_name
          {owner_filter}
          ORDER BY NVL(s.bytes,0) DESC NULLS LAST
        ) WHERE ROWNUM <= 10
    """
    top_rows = _fetch(engine, top_sql, notes, "oracle.data.top_tables", params=params or None)
    if top_rows is not None:
        result["top_tables"] = []
        for row in top_rows:
            name = row.get("qualified_name") or row.get("table_name")
            result["top_tables"].append({
                "table": str(name),
                "size_bytes": _int(row.get("size_bytes")),
                "rows_estimate": _int(row.get("rows_estimate")),
                "column_count": _int(row.get("column_count")),
                "has_pk": bool(row.get("has_pk")),
                "partitioned": str(row.get("partitioned") or "") == "YES",
            })
    else:
        result["top_tables"] = None
    # LOB / LONG columns
    lob_sql = (
        _apply_owner_condition("SELECT c.table_name, c.column_name, c.data_type FROM {p}_tab_columns c WHERE c.data_type IN ('CLOB','NCLOB','BLOB','BFILE','LONG','LONG RAW') AND c.owner = :owner".format(p=prefix), owners, total_bytes_params)
        if prefix == "dba" and owners else
        "SELECT c.table_name, c.column_name, c.data_type FROM {p}_tab_columns c WHERE c.data_type IN ('CLOB','NCLOB','BLOB','BFILE','LONG','LONG RAW')".format(p=prefix))
    lob_rows = _fetch(
        engine,
        lob_sql,
        notes, "oracle.data.lob_columns", params=total_bytes_params or None,
    )
    if lob_rows is not None:
        lob_tables: dict[str, list[dict]] = {}
        for row in lob_rows:
            table = str(row.get("table_name"))
            lob_tables.setdefault(table, []).append({
                "column": str(row.get("column_name")),
                "type": str(row.get("data_type")),
            })
        result["lob_tables"] = [{"table": k, "columns": v} for k, v in sorted(lob_tables.items())]
    else:
        result["lob_tables"] = None
    empty_sql = (
        _apply_owner_condition("SELECT COUNT(*) AS c FROM {p}_tables WHERE (num_rows = 0 OR num_rows IS NULL) AND owner = :owner".format(p=prefix), owners, total_bytes_params)
        if prefix == "dba" and owners else
        "SELECT COUNT(*) AS c FROM {p}_tables WHERE num_rows = 0 OR num_rows IS NULL".format(p=prefix))
    empty_count = _fetch_scalar(
        engine,
        empty_sql,
        notes, "oracle.data.empty_tables", params=total_bytes_params or None,
    )
    result["empty_table_count"] = int(empty_count) if empty_count is not None else None
    no_pk_sql = f"""
        SELECT t.table_name FROM {prefix}_tables t
        WHERE NOT EXISTS (
          SELECT 1 FROM {prefix}_constraints c
          WHERE c.table_name = t.table_name AND c.constraint_type = 'P'
          {("AND c.owner = t.owner" if prefix == "dba" else "")}
        )
        {_apply_owner_condition("AND t.owner = :owner", owners, params) if (prefix == "dba" and owners) else ("AND t.owner = USER" if prefix == "dba" else "")}
        ORDER BY t.table_name
    """
    no_pk_rows = _fetch(engine, no_pk_sql, notes, "oracle.data.no_pk_tables", params=params or None)
    if no_pk_rows is not None:
        result["no_pk_tables"] = [str(row.get("table_name")) for row in no_pk_rows]
    else:
        result["no_pk_tables"] = None
    return result


def _collect_oracle_foreign_keys(engine, notes: list[dict], owners: list[str] | None = None) -> dict[str, Any]:
    prefix = _oracle_scope(engine, notes)
    result: dict[str, Any] = {"scope": "dba" if prefix == "dba" else "user"}
    if owners:
        result["owners"] = owners
    owner_and = ""
    params: dict[str, Any] = {}
    if prefix == "dba" and owners:
        owner_and = _apply_owner_condition(" AND owner = :owner", owners, params)
    count = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM {p}_constraints WHERE constraint_type='R'{owner_and}".format(p=prefix, owner_and=owner_and),
        notes, "oracle.fk.count", params=params or None,
    )
    result["count"] = int(count) if count is not None else None
    join = (
        "c.r_owner = p.owner AND c.r_constraint_name = p.constraint_name"
        if prefix == "dba"
        else "c.r_constraint_name = p.constraint_name"
    )
    dep_sql = f"""
        SELECT c.table_name AS child_table, p.table_name AS parent_table
        FROM {prefix}_constraints c
        JOIN {prefix}_constraints p ON {join}
        WHERE c.constraint_type = 'R'{owner_and}
        FETCH FIRST 20 ROWS ONLY
    """
    rows = _fetch(engine, dep_sql, notes, "oracle.fk.dependencies", params=params or None)
    if rows is not None:
        result["dependencies"] = [
            {"child_table": str(r.get("child_table")), "parent_table": str(r.get("parent_table"))}
            for r in rows
        ]
    else:
        result["dependencies"] = None
    return result


# ---------------------------------------------------------------------------
# MySQL collector
# ---------------------------------------------------------------------------

MYSQL_VARIABLES = (
    "character_set_server",
    "collation_server",
    "lower_case_table_names",
    "innodb_buffer_pool_size",
    "innodb_log_file_size",
    "innodb_flush_log_at_trx_commit",
    "max_connections",
    "time_zone",
    "system_time_zone",
    "sql_mode",
    "binlog_format",
    "log_bin",
    "transaction_isolation",
    "tx_isolation",
    "wait_timeout",
    "interactive_timeout",
    "default_storage_engine",
    "version",
    "version_comment",
    "server_id",
    "innodb_data_file_path",
    "character_set_database",
    "collation_database",
)

MYSQL_LOB_TYPES = (
    "tinyblob", "blob", "mediumblob", "longblob",
    "tinytext", "text", "mediumtext", "longtext", "json",
)


def _collect_mysql(engine, config: ConnectionConfig, notes: list[dict]) -> dict[str, Any]:
    env: dict[str, Any] = {"dialect": "mysql", "notes": []}
    version_row = _fetch_one(
        engine,
        "SELECT VERSION() AS version, @@hostname AS hostname, @@port AS port, @@version_comment AS comment, @@server_id AS server_id",
        notes, "mysql.instance",
    )
    if version_row:
        env["version"] = _safe_str(version_row.get("version"))
        env["host"] = _safe_str(version_row.get("hostname"))
        env["database"] = config.database
        env["port"] = _int(version_row.get("port"))
        env["version_comment"] = _safe_str(version_row.get("comment"))
    env["parameters"] = {}
    rows = _fetch(
        engine,
        f"SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_variables WHERE VARIABLE_NAME IN {_in_clause([v.upper() for v in MYSQL_VARIABLES])}",
        notes, "mysql.variables",
    )
    if rows is not None:
        for row in rows:
            key = str(row.get("VARIABLE_NAME")).upper()
            env["parameters"][key] = _safe_str(row.get("VARIABLE_VALUE"))
    # merge tx_isolation / transaction_isolation
    if "TRANSACTION_ISOLATION" not in env["parameters"] and "TX_ISOLATION" in env["parameters"]:
        env["parameters"]["TRANSACTION_ISOLATION"] = env["parameters"].get("TX_ISOLATION")
    env["charset"] = env["parameters"].get("CHARACTER_SET_SERVER") or env["parameters"].get("CHARACTER_SET_DATABASE")
    env["collation"] = env["parameters"].get("COLLATION_SERVER") or env["parameters"].get("COLLATION_DATABASE")
    # startup time from Uptime
    uptime = _fetch_one(engine, "SHOW GLOBAL STATUS LIKE 'Uptime'", notes, "mysql.uptime")
    if uptime and uptime.get("Value") is not None:
        seconds = _int(uptime.get("Value"))
        if seconds is not None:
            env["startup_time"] = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat(sep=" ")
            env["uptime_seconds"] = seconds
    # run mode: replica or primary
    try:
        with engine.connect() as conn:
            slave = conn.execute(text("SHOW SLAVE STATUS")).mappings().all()
            if slave:
                env["run_mode"] = "从库（replica）"
            else:
                master = conn.execute(text("SHOW MASTER STATUS")).mappings().all()
                env["run_mode"] = "主库（master）" if master else "单实例/未知"
    except Exception as exc:  # noqa: BLE001
        notes.append({"section": "mysql.run_mode", "message": f"{type(exc).__name__}: {str(exc)[:200]}"})
        env["run_mode"] = None
    # host resources are not exposed to ordinary MySQL accounts
    env["host_resources"] = None
    notes.append({"section": "mysql.host_resources", "message": "MySQL 不暴露主机 CPU/内存信息，主机资源项为空"})
    return env


def _collect_mysql_objects(engine, notes: list[dict]) -> dict[str, Any]:
    result: dict[str, Any] = {"scope": "database", "counts": {}}
    queries = {
        "tables": "SELECT COUNT(*) AS c FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE'",
        "views": "SELECT COUNT(*) AS c FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='VIEW'",
        "procedures": "SELECT COUNT(*) AS c FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=DATABASE() AND ROUTINE_TYPE='PROCEDURE'",
        "functions": "SELECT COUNT(*) AS c FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=DATABASE() AND ROUTINE_TYPE='FUNCTION'",
        "triggers": "SELECT COUNT(*) AS c FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE()",
        "indexes": "SELECT COUNT(*) AS c FROM (SELECT DISTINCT TABLE_NAME, INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE()) t",
        "constraints": "SELECT COUNT(*) AS c FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND CONSTRAINT_TYPE IN ('PRIMARY KEY','UNIQUE','FOREIGN KEY')",
        "partitioned_tables": "SELECT COUNT(*) AS c FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE' AND CREATE_OPTIONS LIKE '%partitioned%'",
        "events": "SELECT COUNT(*) AS c FROM information_schema.EVENTS WHERE EVENT_SCHEMA=DATABASE()",
    }
    for key, sql in queries.items():
        count = _fetch_scalar(engine, sql, notes, f"mysql.objects.{key}")
        result["counts"][key] = int(count) if count is not None else None
    for unsupported in ("sequences", "synonyms", "dblinks", "materialized_views"):
        result["counts"][unsupported] = None
        notes.append({
            "section": f"mysql.objects.{unsupported}",
            "message": f"MySQL 无原生{unsupported}对象，统计为 null",
        })
    result["details"] = _collect_mysql_details(engine, notes)
    return result


def _collect_mysql_details(engine, notes: list[dict]) -> dict[str, Any]:
    """P1: object detail lists for MySQL (routines / triggers; sequences/synonyms/dblinks unsupported)."""
    result: dict[str, Any] = {"scope": "database"}
    for unsupported in ("sequences", "synonyms", "dblinks"):
        result[unsupported] = None
        notes.append({"section": f"mysql.details.{unsupported}", "message": f"MySQL 无原生{unsupported}对象，明细为 null"})

    proc_rows = _fetch(engine, """
        SELECT ROUTINE_NAME, ROUTINE_TYPE, CREATED
        FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=DATABASE()
        ORDER BY ROUTINE_TYPE, ROUTINE_NAME LIMIT {limit}""".format(limit=DETAIL_LIST_LIMIT + 1),
        notes, "mysql.details.routines")
    proc_count = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=DATABASE()",
                               notes, "mysql.details.routines.count")
    result["procedures"] = _truncated_detail(proc_rows, proc_count)
    if proc_count not in (None, 0):
        notes.append({"section": "mysql.details.routines", "message": "MySQL 存储过程/函数无 VALID/INVALID 状态与行数概念，仅提供名称与创建时间"})

    trig_rows = _fetch(engine, """
        SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION
        FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE()
        ORDER BY TRIGGER_NAME LIMIT {limit}""".format(limit=DETAIL_LIST_LIMIT + 1),
        notes, "mysql.details.triggers")
    trig_count = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE()",
                               notes, "mysql.details.triggers.count")
    result["triggers"] = _truncated_detail(trig_rows, trig_count)
    return result


def _collect_mysql_data(engine, notes: list[dict]) -> dict[str, Any]:
    result: dict[str, Any] = {"scope": "database"}
    total_bytes = _fetch_scalar(
        engine,
        "SELECT IFNULL(SUM(data_length+index_length),0) AS c FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()",
        notes, "mysql.data.total_bytes",
    )
    result["total_bytes"] = int(total_bytes) if total_bytes is not None else None
    total_rows = _fetch_scalar(
        engine,
        "SELECT IFNULL(SUM(TABLE_ROWS),0) AS c FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()",
        notes, "mysql.data.total_rows",
    )
    result["total_rows_estimate"] = int(total_rows) if total_rows is not None else None
    top_rows = _fetch(
        engine,
        "SELECT TABLE_NAME, TABLE_ROWS, data_length+index_length AS size_bytes FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE' ORDER BY size_bytes DESC LIMIT 10",
        notes, "mysql.data.top_tables",
    )
    column_map: dict[str, int] = {}
    col_rows = _fetch(
        engine,
        "SELECT TABLE_NAME, COUNT(*) AS c FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() GROUP BY TABLE_NAME",
        notes, "mysql.data.column_counts",
    )
    if col_rows:
        column_map = {str(r["TABLE_NAME"]): int(r["c"]) for r in col_rows}
    pk_set: set[str] = set()
    pk_rows = _fetch(
        engine,
        "SELECT TABLE_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND CONSTRAINT_TYPE='PRIMARY KEY'",
        notes, "mysql.data.pk_tables",
    )
    if pk_rows:
        pk_set = {str(r["TABLE_NAME"]) for r in pk_rows}
    if top_rows is not None:
        result["top_tables"] = [
            {
                "table": str(r.get("TABLE_NAME")),
                "size_bytes": _int(r.get("size_bytes")),
                "rows_estimate": _int(r.get("TABLE_ROWS")),
                "column_count": column_map.get(str(r.get("TABLE_NAME"))),
                "has_pk": str(r.get("TABLE_NAME")) in pk_set,
            }
            for r in top_rows
        ]
    else:
        result["top_tables"] = None
    lob_rows = _fetch(
        engine,
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND DATA_TYPE IN :types ORDER BY TABLE_NAME, ORDINAL_POSITION",
        notes, "mysql.data.lob_columns", {"types": [t.upper() for t in MYSQL_LOB_TYPES]},
    )
    if lob_rows is not None:
        lob_tables: dict[str, list[dict]] = {}
        for row in lob_rows:
            table = str(row.get("TABLE_NAME"))
            lob_tables.setdefault(table, []).append({
                "column": str(row.get("COLUMN_NAME")),
                "type": str(row.get("DATA_TYPE")).lower(),
            })
        result["lob_tables"] = [{"table": k, "columns": v} for k, v in sorted(lob_tables.items())]
    else:
        result["lob_tables"] = None
    empty_count = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE' AND TABLE_ROWS=0",
        notes, "mysql.data.empty_tables",
    )
    result["empty_table_count"] = int(empty_count) if empty_count is not None else None
    no_pk_rows = _fetch(
        engine,
        """SELECT t.TABLE_NAME FROM information_schema.TABLES t
           LEFT JOIN information_schema.TABLE_CONSTRAINTS c
             ON c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME AND c.CONSTRAINT_TYPE='PRIMARY KEY'
           WHERE t.TABLE_SCHEMA=DATABASE() AND t.TABLE_TYPE='BASE TABLE' AND c.CONSTRAINT_NAME IS NULL
           ORDER BY t.TABLE_NAME""",
        notes, "mysql.data.no_pk_tables",
    )
    if no_pk_rows is not None:
        result["no_pk_tables"] = [str(r.get("TABLE_NAME")) for r in no_pk_rows]
    else:
        result["no_pk_tables"] = None
    return result


def _collect_mysql_foreign_keys(engine, notes: list[dict]) -> dict[str, Any]:
    result: dict[str, Any] = {"scope": "database"}
    count = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL",
        notes, "mysql.fk.count",
    )
    result["count"] = int(count) if count is not None else None
    rows = _fetch(
        engine,
        """SELECT TABLE_NAME AS child_table, REFERENCED_TABLE_NAME AS parent_table
           FROM information_schema.KEY_COLUMN_USAGE
           WHERE TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL
           GROUP BY TABLE_NAME, REFERENCED_TABLE_NAME LIMIT 20""",
        notes, "mysql.fk.dependencies",
    )
    if rows is not None:
        result["dependencies"] = [
            {"child_table": str(r.get("child_table")), "parent_table": str(r.get("parent_table"))}
            for r in rows
        ]
    else:
        result["dependencies"] = None
    return result


# ---------------------------------------------------------------------------
# PostgreSQL collector
# ---------------------------------------------------------------------------

PG_PARAMETERS = (
    "server_encoding",
    "lc_collate",
    "lc_ctype",
    "shared_buffers",
    "work_mem",
    "maintenance_work_mem",
    "max_connections",
    "timezone",
    "wal_level",
    "max_wal_senders",
    "synchronous_commit",
    "default_transaction_isolation",
    "standard_conforming_strings",
    "statement_timeout",
    "effective_cache_size",
    "fsync",
    "full_page_writes",
    "max_worker_processes",
    "max_parallel_workers",
    "checkpoint_completion_target",
)


def _collect_postgresql(engine, config: ConnectionConfig, notes: list[dict]) -> dict[str, Any]:
    env: dict[str, Any] = {"dialect": "postgresql", "notes": []}
    info = _fetch_one(
        engine,
        "SELECT version() AS version, current_database() AS database, inet_server_addr() AS host, inet_server_port() AS port, pg_postmaster_start_time() AS startup_time",
        notes, "pg.instance",
    )
    if info:
        env["version"] = _safe_str(info.get("version"))
        env["host"] = _safe_str(info.get("host"))
        env["database"] = _safe_str(info.get("database")) or config.database
        env["port"] = _int(info.get("port"))
        env["startup_time"] = _iso(info.get("startup_time"))
    env["parameters"] = {}
    rows = _fetch(
        engine,
        f"SELECT name, setting, unit FROM pg_settings WHERE name IN {_in_clause(PG_PARAMETERS)}",
        notes, "pg.parameters",
    )
    if rows is not None:
        for row in rows:
            name = str(row.get("name") or "").upper()
            value = _safe_str(row.get("setting"))
            unit = _safe_str(row.get("unit"))
            env["parameters"][name] = f"{value}{unit or ''}"
    env["charset"] = env["parameters"].get("SERVER_ENCODING")
    env["collation"] = env["parameters"].get("LC_COLLATE")
    recovery = _fetch_one(engine, "SELECT pg_is_in_recovery() AS in_recovery", notes, "pg.recovery")
    standby_count = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM pg_stat_replication",
        notes, "pg.replication",
    )
    if recovery and recovery.get("in_recovery") is True:
        env["run_mode"] = "备库（hot standby / recovery）"
    elif standby_count is not None and standby_count > 0:
        env["run_mode"] = f"主库（{standby_count} 个流复制备库）"
    else:
        env["run_mode"] = "单实例/无流复制"
    env["host_resources"] = None
    notes.append({"section": "pg.host_resources", "message": "PostgreSQL 元数据不暴露主机 CPU/内存信息，主机资源项为空"})
    return env


def _collect_postgresql_objects(engine, notes: list[dict]) -> dict[str, Any]:
    result: dict[str, Any] = {"scope": "current_database", "counts": {}}
    queries = {
        "tables": "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema NOT IN {sys} AND table_type='BASE TABLE'".format(sys=SYSTEM_SCHEMAS_SQL),
        "views": "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema NOT IN {sys} AND table_type='VIEW'".format(sys=SYSTEM_SCHEMAS_SQL),
        "sequences": "SELECT COUNT(*) AS c FROM information_schema.sequences WHERE sequence_schema NOT IN {sys}".format(sys=SYSTEM_SCHEMAS_SQL),
        "procedures": "SELECT COUNT(*) AS c FROM information_schema.routines WHERE routine_schema NOT IN {sys} AND routine_type='PROCEDURE'".format(sys=SYSTEM_SCHEMAS_SQL),
        "functions": "SELECT COUNT(*) AS c FROM information_schema.routines WHERE routine_schema NOT IN {sys} AND routine_type='FUNCTION'".format(sys=SYSTEM_SCHEMAS_SQL),
        "triggers": "SELECT COUNT(*) AS c FROM information_schema.triggers WHERE trigger_schema NOT IN {sys}".format(sys=SYSTEM_SCHEMAS_SQL),
        "materialized_views": "SELECT COUNT(*) AS c FROM pg_matviews WHERE schemaname NOT IN {sys}".format(sys=SYSTEM_SCHEMAS_SQL),
        "indexes": "SELECT COUNT(*) AS c FROM pg_indexes WHERE schemaname NOT IN {sys}".format(sys=SYSTEM_SCHEMAS_SQL),
        "constraints": "SELECT COUNT(*) AS c FROM information_schema.table_constraints WHERE constraint_schema NOT IN {sys}".format(sys=SYSTEM_SCHEMAS_SQL),
        "partitioned_tables": "SELECT COUNT(*) AS c FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN {sys} AND c.relkind='p'".format(sys=SYSTEM_SCHEMAS_SQL),
    }
    for key, sql in queries.items():
        count = _fetch_scalar(engine, sql, notes, f"pg.objects.{key}")
        result["counts"][key] = int(count) if count is not None else None
    # synonyms: PG has no native synonyms
    result["counts"]["synonyms"] = None
    notes.append({"section": "pg.objects.synonyms", "message": "PostgreSQL 无原生同义词对象，统计为 null"})
    # dblink: approximate with foreign tables + dblink extension
    foreign_count = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM information_schema.foreign_tables",
        notes, "pg.objects.foreign_tables",
    )
    dblink_ext = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM pg_extension WHERE extname IN ('dblink','postgres_fdw')",
        notes, "pg.objects.dblink_extension",
    )
    if foreign_count is not None or dblink_ext is not None:
        result["counts"]["dblinks"] = int(foreign_count or 0) + int(dblink_ext or 0)
        result["dblink_note"] = "PostgreSQL 无原生 DBLINK，统计为外部表(FDW)数量 + dblink/postgres_fdw 扩展数"
    else:
        result["counts"]["dblinks"] = None
    # scheduler jobs: pg_cron extension presence (actual cron.job may be unreadable)
    cron_ext = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM pg_extension WHERE extname='pg_cron'", notes, "pg.objects.pg_cron")
    if cron_ext is not None and cron_ext > 0:
        job_count = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM cron.job", notes, "pg.objects.cron_jobs")
        result["counts"]["scheduler_jobs"] = int(job_count) if job_count is not None else None
        if job_count is None:
            notes.append({"section": "pg.objects.cron_jobs", "message": "检测到 pg_cron 扩展但无法读取 cron.job 表，定时任务数未知"})
    else:
        result["counts"]["scheduler_jobs"] = 0 if cron_ext is not None else None
    result["details"] = _collect_postgresql_details(engine, notes)
    return result


def _collect_postgresql_details(engine, notes: list[dict]) -> dict[str, Any]:
    """P1: object detail lists for PostgreSQL (sequences / routines / triggers)."""
    result: dict[str, Any] = {"scope": "current_database"}
    sys_sql = SYSTEM_SCHEMAS_SQL

    seq_rows = _fetch(engine, f"""
        SELECT sequence_schema, sequence_name, start_value, increment, cache_size, cycle
        FROM information_schema.sequences WHERE sequence_schema NOT IN {sys_sql}
        ORDER BY sequence_schema, sequence_name LIMIT {DETAIL_LIST_LIMIT + 1}""",
        notes, "pg.details.sequences")
    seq_count = _fetch_scalar(engine, f"SELECT COUNT(*) AS c FROM information_schema.sequences WHERE sequence_schema NOT IN {sys_sql}",
                              notes, "pg.details.sequences.count")
    result["sequences"] = _truncated_detail(seq_rows, seq_count)

    result["synonyms"] = None
    result["dblinks"] = None
    notes.append({"section": "pg.details.synonyms", "message": "PostgreSQL 无原生同义词对象，明细为 null"})
    notes.append({"section": "pg.details.dblinks", "message": "PostgreSQL 无原生 DBLINK，明细为 null（FDW/扩展请按对象统计中的 dblink_note 处理）"})

    proc_rows = _fetch(engine, f"""
        SELECT n.nspname AS routine_schema, p.proname AS routine_name,
               CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END AS routine_type,
               pg_get_function_identity_arguments(p.oid) AS arguments
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN {sys_sql} AND p.prokind IN ('f','p')
        ORDER BY routine_type, n.nspname, p.proname LIMIT {DETAIL_LIST_LIMIT + 1}""",
        notes, "pg.details.routines")
    proc_count = _fetch_scalar(engine, f"""
        SELECT COUNT(*) AS c FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN {sys_sql} AND p.prokind IN ('f','p')""",
        notes, "pg.details.routines.count")
    result["procedures"] = _truncated_detail(proc_rows, proc_count)
    if proc_count not in (None, 0):
        notes.append({"section": "pg.details.routines", "message": "PostgreSQL 存储过程/函数无 VALID/INVALID 状态概念，PG 11+ 均视为可用；仅提供名称/模式/签名"})

    trig_rows = _fetch(engine, f"""
        SELECT t.tgname AS trigger_name,
               n.nspname AS trigger_schema,
               c.relname AS event_object_table,
               CASE WHEN (t.tgtype::int & 2) = 2 THEN 'BEFORE' ELSE 'AFTER' END AS action_timing,
               CASE WHEN (t.tgtype::int & 4) = 4 THEN 'ROW' ELSE 'STATEMENT' END AS action_orientation,
               CASE t.tgenabled WHEN 'O' THEN 'enabled' ELSE 'disabled' END AS enabled,
               (SELECT string_agg(ev, ',') FROM (VALUES
                   (CASE WHEN (t.tgtype::int & 16) <> 0 THEN 'INSERT' END),
                   (CASE WHEN (t.tgtype::int & 32) <> 0 THEN 'UPDATE' END),
                   (CASE WHEN (t.tgtype::int & 64) <> 0 THEN 'DELETE' END),
                   (CASE WHEN (t.tgtype::int & 128) <> 0 THEN 'TRUNCATE' END)) AS v(ev)
                WHERE ev IS NOT NULL) AS triggering_event
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT IN {sys_sql} AND NOT t.tgisinternal
        ORDER BY n.nspname, t.tgname LIMIT {DETAIL_LIST_LIMIT + 1}""",
        notes, "pg.details.triggers")
    trig_count = _fetch_scalar(engine, f"""
        SELECT COUNT(*) AS c FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT IN {sys_sql} AND NOT t.tgisinternal""",
        notes, "pg.details.triggers.count")
    result["triggers"] = _truncated_detail(trig_rows, trig_count)
    return result


def _collect_postgresql_data(engine, notes: list[dict]) -> dict[str, Any]:
    result: dict[str, Any] = {"scope": "current_database"}
    total_bytes = _fetch_scalar(
        engine,
        "SELECT pg_database_size(current_database()) AS c",
        notes, "pg.data.total_bytes",
    )
    result["total_bytes"] = int(total_bytes) if total_bytes is not None else None
    total_rows = _fetch_scalar(
        engine,
        "SELECT COALESCE(SUM(GREATEST(c.reltuples,0)),0) AS c FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN {sys}".format(sys=SYSTEM_SCHEMAS_SQL),
        notes, "pg.data.total_rows",
    )
    result["total_rows_estimate"] = int(total_rows) if total_rows is not None else None
    top_rows = _fetch(
        engine,
        """SELECT n.nspname AS schema_name, c.relname AS table_name,
                  pg_total_relation_size(c.oid) AS size_bytes,
                  GREATEST(c.reltuples::bigint, 0) AS rows_estimate,
                  (SELECT COUNT(*) FROM pg_attribute a
                    WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS column_count,
                  EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='p') AS has_pk
           FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
           WHERE c.relkind='r' AND n.nspname NOT IN {sys}
           ORDER BY size_bytes DESC LIMIT 10""".format(sys=SYSTEM_SCHEMAS_SQL),
        notes, "pg.data.top_tables",
    )
    if top_rows is not None:
        result["top_tables"] = [
            {
                "table": f"{r.get('schema_name')}.{r.get('table_name')}",
                "size_bytes": _int(r.get("size_bytes")),
                "rows_estimate": _int(r.get("rows_estimate")),
                "column_count": _int(r.get("column_count")),
                "has_pk": bool(r.get("has_pk")),
            }
            for r in top_rows
        ]
    else:
        result["top_tables"] = None
    lob_rows = _fetch(
        engine,
        """SELECT table_schema, table_name, column_name, data_type
           FROM information_schema.columns
           WHERE table_schema NOT IN {sys} AND data_type IN ('text','bytea')
           ORDER BY table_schema, table_name, ordinal_position""".format(sys=SYSTEM_SCHEMAS_SQL),
        notes, "pg.data.lob_columns",
    )
    if lob_rows is not None:
        lob_tables: dict[str, list[dict]] = {}
        for row in lob_rows:
            table = f"{row.get('table_schema')}.{row.get('table_name')}"
            lob_tables.setdefault(table, []).append({
                "column": str(row.get("column_name")),
                "type": str(row.get("data_type")),
            })
        result["lob_tables"] = [{"table": k, "columns": v} for k, v in sorted(lob_tables.items())]
        if lob_rows:
            result["lob_note"] = "PostgreSQL 无原生 LOB 列类型，text/bytea 视为大字段统计"
    else:
        result["lob_tables"] = None
    large_objects = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM pg_largeobject", notes, "pg.data.large_objects")
    result["large_object_chunks"] = int(large_objects) if large_objects is not None else None
    empty_count = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN {sys} AND c.reltuples=0".format(sys=SYSTEM_SCHEMAS_SQL),
        notes, "pg.data.empty_tables",
    )
    result["empty_table_count"] = int(empty_count) if empty_count is not None else None
    no_pk_rows = _fetch(
        engine,
        """SELECT n.nspname AS schema_name, c.relname AS table_name
           FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
           WHERE c.relkind='r' AND n.nspname NOT IN {sys}
             AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='p')
           ORDER BY n.nspname, c.relname""".format(sys=SYSTEM_SCHEMAS_SQL),
        notes, "pg.data.no_pk_tables",
    )
    if no_pk_rows is not None:
        result["no_pk_tables"] = [f"{r.get('schema_name')}.{r.get('table_name')}" for r in no_pk_rows]
    else:
        result["no_pk_tables"] = None
    return result


def _collect_postgresql_foreign_keys(engine, notes: list[dict]) -> dict[str, Any]:
    result: dict[str, Any] = {"scope": "current_database"}
    count = _fetch_scalar(
        engine,
        "SELECT COUNT(*) AS c FROM pg_constraint f JOIN pg_class c ON c.oid=f.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE f.contype='f' AND n.nspname NOT IN {sys}".format(sys=SYSTEM_SCHEMAS_SQL),
        notes, "pg.fk.count",
    )
    result["count"] = int(count) if count is not None else None
    rows = _fetch(
        engine,
        """SELECT child.relname AS child_table, parent.relname AS parent_table
           FROM pg_constraint f
           JOIN pg_class child ON child.oid=f.conrelid
           JOIN pg_class parent ON parent.oid=f.confrelid
           JOIN pg_namespace n ON n.oid=child.relnamespace
           WHERE f.contype='f' AND n.nspname NOT IN {sys}
           LIMIT 20""".format(sys=SYSTEM_SCHEMAS_SQL),
        notes, "pg.fk.dependencies",
    )
    if rows is not None:
        result["dependencies"] = [
            {"child_table": str(r.get("child_table")), "parent_table": str(r.get("parent_table"))}
            for r in rows
        ]
    else:
        result["dependencies"] = None
    return result


# ---------------------------------------------------------------------------
# dialect dispatch
# ---------------------------------------------------------------------------

_COLLECTORS = {
    "oracle": (
        _collect_oracle,
        _collect_oracle_objects,
        _collect_oracle_data,
        _collect_oracle_foreign_keys,
    ),
    "mysql": (
        _collect_mysql,
        _collect_mysql_objects,
        _collect_mysql_data,
        _collect_mysql_foreign_keys,
    ),
    # TDSQL 兼容 MySQL 协议与元数据视图，复用 mysql 收集器
    "tdsql": (
        _collect_mysql,
        _collect_mysql_objects,
        _collect_mysql_data,
        _collect_mysql_foreign_keys,
    ),
    "postgresql": (
        _collect_postgresql,
        _collect_postgresql_objects,
        _collect_postgresql_data,
        _collect_postgresql_foreign_keys,
    ),
}


def _collect_side(config: ConnectionConfig, side: str, owners: list[str] | None = None) -> dict[str, Any]:
    notes: list[dict] = []
    engine = make_engine(config)
    try:
        # Liveness probe: fail fast when the database itself is unreachable,
        # so connection errors surface as connect_error + blocking risk instead
        # of silently degrading every collector to null.
        probe_sql = "SELECT 1 FROM DUAL" if config.type == "oracle" else "SELECT 1"
        with engine.connect() as conn:
            conn.execute(text(probe_sql))
        collect_env, collect_objects, collect_data, collect_fk = _COLLECTORS[config.type]
        env = collect_env(engine, config, notes)
        env["notes"] = notes
        if config.type == "oracle":
            return {
                "env": env,
                "objects": collect_objects(engine, notes, owners=owners),
                "data": collect_data(engine, notes, owners=owners),
                "foreign_keys": collect_fk(engine, notes, owners=owners),
                "partition": _collect_oracle_partitions(engine, notes, owners=owners),
            }
        return {
            "env": env,
            "objects": collect_objects(engine, notes),
            "data": collect_data(engine, notes),
            "foreign_keys": collect_fk(engine, notes),
        }
    except Exception as exc:  # noqa: BLE001 - connection-level failures degrade gracefully
        notes.append({"section": "connect", "message": f"{type(exc).__name__}: {str(exc)[:400]}"})
        return {
            "env": {"dialect": config.type, "notes": notes, "parameters": {}},
            "objects": {"counts": {}, "scope": "unavailable"},
            "data": {"scope": "unavailable"},
            "foreign_keys": {"count": None, "dependencies": None},
            "connect_error": str(exc)[:400],
        }
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# P1: per-column type mapping preview for TOP tables
# ---------------------------------------------------------------------------


def _augment_top_table_mappings(side_data: dict, config: ConnectionConfig, target_dialect: str, notes: list[dict]) -> None:
    """Attach per-column portable_type mappings (with degradation markers) to TOP tables."""
    top = side_data.get("top_tables")
    if not isinstance(top, list) or not top:
        return
    engine = make_engine(config)
    try:
        inspector = inspect(engine)
        target_compiler = _dialect_compile(target_dialect)
        schema = default_schema(config)
        for table in top:
            raw_name = table.get("table")
            if not raw_name:
                continue
            schema_use, name_use = schema, str(raw_name)
            if "." in str(raw_name):
                schema_use, name_use = str(raw_name).split(".", 1)
            try:
                columns = inspector.get_columns(name_use, schema=schema_use)
            except Exception as exc:  # noqa: BLE001 - per-table degradation
                notes.append({"section": "deep.top_table_mappings",
                              "message": f"表 {raw_name} 列映射读取失败：{type(exc).__name__}: {str(exc)[:200]}"})
                continue
            mappings: list[dict] = []
            for column in columns:
                identity = bool(column.get("identity")) or column.get("autoincrement") is True
                info = portable_type_info(column["type"], target_dialect, identity)
                mapped = info["target_type"]
                try:
                    target_ddl = str(mapped.compile(dialect=target_compiler))
                except Exception:  # noqa: BLE001 - fall back to repr
                    target_ddl = str(mapped)
                mappings.append({
                    "column": column["name"],
                    "source_type": str(column["type"]),
                    "target_type": target_ddl,
                    "degraded": info["degraded"],
                    "degradation": info["degradation"],
                })
            table["column_mappings"] = mappings
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# P1: lightweight data quality pre-check
# ---------------------------------------------------------------------------

QUALITY_MAX_TABLES = 20
QUALITY_MAX_ROWS = 200_000
QUALITY_SAMPLE_ROWS = 1_000
QUALITY_NULL_RATE_THRESHOLD = 0.30
# MySQL utf8mb4 VARCHAR character limit; used as the "target upper bound" guard.
QUALITY_OVERLONG_VARCHAR_LIMIT = 16_383


def _quality_candidate_tables(engine, config: ConnectionConfig, notes: list[dict]) -> list[dict] | None:
    """Top tables with estimated rows <= 200k (max 20), ordered by size."""
    params = {"max_rows": QUALITY_MAX_ROWS, "limit": QUALITY_MAX_TABLES}
    if config.type == "oracle":
        prefix = _oracle_scope(engine, notes)
        owner_filter = "AND t.owner = USER" if prefix == "dba" else ""
        sql = f"""
            SELECT t.table_name AS table_name, NVL(t.num_rows,0) AS rows_estimate
            FROM {prefix}_tables t
            WHERE NVL(t.num_rows,0) <= :max_rows {owner_filter}
            ORDER BY NVL(t.num_rows,0) DESC FETCH FIRST :limit ROWS ONLY"""
        rows = _fetch(engine, sql, notes, "quality.candidates", params)
        if rows is None:
            return None
        return [{"name": str(r["table_name"]), "rows_estimate": int(r["rows_estimate"])} for r in rows]
    if config.type == "mysql":
        sql = """
            SELECT TABLE_NAME AS table_name, IFNULL(TABLE_ROWS,0) AS rows_estimate
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE' AND IFNULL(TABLE_ROWS,0) <= :max_rows
            ORDER BY IFNULL(TABLE_ROWS,0) DESC LIMIT :limit"""
        rows = _fetch(engine, sql, notes, "quality.candidates", params)
        if rows is None:
            return None
        return [{"name": str(r["table_name"]), "rows_estimate": int(r["rows_estimate"])} for r in rows]
    sql = f"""
        SELECT n.nspname AS schema_name, c.relname AS table_name,
               GREATEST(c.reltuples::bigint,0) AS rows_estimate
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND n.nspname NOT IN {SYSTEM_SCHEMAS_SQL}
          AND GREATEST(c.reltuples::bigint,0) <= :max_rows
        ORDER BY GREATEST(c.reltuples::bigint,0) DESC LIMIT :limit"""
    rows = _fetch(engine, sql, notes, "quality.candidates", params)
    if rows is None:
        return None
    return [{"name": f"{r['schema_name']}.{r['table_name']}", "rows_estimate": int(r["rows_estimate"])} for r in rows]


def _quality_check_table(
    engine, config: ConnectionConfig, table_name: str, schema: str, target_names: set[str], notes: list[dict],
) -> dict[str, Any]:
    """Run one lightweight quality check against a single table."""
    result: dict[str, Any] = {"table": table_name, "rows": None, "risks": [], "checks": {
        "null_rate": [], "duplicates": None, "overlong": [], "encoding": [], "unique_conflict": None,
    }}
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name, schema=schema)
        if not columns:
            result["risks"].append({"level": "warning", "code": "QUALITY_NO_COLUMNS", "message": "无法读取表结构，跳过该表预检"})
            return result
        meta = MetaData()
        tbl = Table(table_name, meta, autoload_with=engine, schema=schema)

        # row count
        try:
            with engine.connect() as conn:
                result["rows"] = int(conn.scalar(select(func.count()).select_from(tbl)) or 0)
        except Exception as exc:  # noqa: BLE001
            notes.append({"section": "quality.rows", "message": f"表 {table_name} 行数统计失败：{type(exc).__name__}: {str(exc)[:200]}"})

        # 1. null rate on nullable non-pk columns
        null_targets = [c for c in tbl.columns if c.nullable and not c.primary_key]
        if null_targets and result["rows"] is not None:
            try:
                exprs = [func.count().label("__total__")] + [func.count(c).label(c.name) for c in null_targets]
                with engine.connect() as conn:
                    row = conn.execute(select(*exprs)).mappings().first()
                total = int(row["__total__"]) if row else 0
                if total > 0:
                    for c in null_targets:
                        nonnull = int(row[c.name])
                        rate = (total - nonnull) / total
                        if rate > QUALITY_NULL_RATE_THRESHOLD:
                            result["checks"]["null_rate"].append({"column": c.name, "null_rate": round(rate, 2)})
            except Exception as exc:  # noqa: BLE001
                notes.append({"section": "quality.null_rate", "message": f"表 {table_name} 空值率统计失败：{type(exc).__name__}: {str(exc)[:200]}"})

        # 2. duplicate detection on pk / unique keys (conclusion only, no row listing)
        try:
            pk_cols = inspector.get_pk_constraint(table_name, schema=schema).get("constrained_columns") or []
            unique_sets: list[tuple[str, list[str]]] = []
            if pk_cols:
                unique_sets.append(("PRIMARY", pk_cols))
            for uq in inspector.get_unique_constraints(table_name, schema=schema):
                ucols = uq.get("column_names") or []
                if ucols:
                    unique_sets.append((str(uq.get("name") or "UNIQUE"), ucols))
            duplicates = None
            if unique_sets:
                found = False
                with engine.connect() as conn:
                    for key_name, key_cols in unique_sets:
                        missing = [k for k in key_cols if k not in tbl.c]
                        if missing:
                            continue
                        group_cols = [tbl.c[k] for k in key_cols]
                        stmt = select(*group_cols, func.count().label("__c__")).select_from(tbl) \
                            .group_by(*group_cols).having(func.count() > 1).limit(1)
                        try:
                            dup_row = conn.execute(stmt).mappings().first()
                        except Exception as exc:  # noqa: BLE001
                            notes.append({"section": "quality.duplicates", "message": f"表 {table_name} 唯一键 {key_name} 重复检测失败：{type(exc).__name__}: {str(exc)[:200]}"})
                            continue
                        if dup_row is not None:
                            duplicates = {"exists": True, "key": key_name, "columns": key_cols}
                            found = True
                            break
                if not found:
                    duplicates = {"exists": False}
            result["checks"]["duplicates"] = duplicates
        except Exception as exc:  # noqa: BLE001
            notes.append({"section": "quality.duplicates", "message": f"表 {table_name} 重复检测失败：{type(exc).__name__}: {str(exc)[:200]}"})

        # 3. overlong char columns (near / above target VARCHAR upper bound)
        try:
            overlong = []
            for col in columns:
                col_type = col["type"]
                if isinstance(col_type, String) or col_type.__class__.__name__.upper() in ("CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "NVARCHAR2", "STRING"):
                    length = getattr(col_type, "length", None)
                    if length and int(length) > QUALITY_OVERLONG_VARCHAR_LIMIT:
                        overlong.append({
                            "column": col["name"],
                            "source_length": int(length),
                            "target_type": "VARCHAR(16383)（MySQL utf8mb4 字符上限）",
                        })
            result["checks"]["overlong"] = overlong
        except Exception as exc:  # noqa: BLE001
            notes.append({"section": "quality.overlong", "message": f"表 {table_name} 超长字段检测失败：{type(exc).__name__}: {str(exc)[:200]}"})

        # 4. encoding anomaly sample (control chars / surrogate chars)
        try:
            char_cols = [c for c in tbl.columns if isinstance(c.type, String)]
            issues: list[dict] = []
            if char_cols:
                with engine.connect() as conn:
                    rows = conn.execute(select(*char_cols).limit(QUALITY_SAMPLE_ROWS)).mappings().all()
                for row in rows:
                    for c in char_cols:
                        value = row[c.name]
                        if not isinstance(value, str):
                            continue
                        issue = None
                        for ch in value:
                            code = ord(ch)
                            if code < 32 and ch not in "\t\n\r":
                                issue = "不可见控制字符"
                                break
                            if 0xD800 <= code <= 0xDFFF:
                                issue = "代理区字符"
                                break
                        if issue:
                            issues.append({"column": c.name, "issue": issue})
                            break
            result["checks"]["encoding"] = issues
        except Exception as exc:  # noqa: BLE001
            notes.append({"section": "quality.encoding", "message": f"表 {table_name} 编码采样失败：{type(exc).__name__}: {str(exc)[:200]}"})

        # 5. unique-conflict pre-check against existing target tables
        try:
            plain_name = str(table_name).split(".")[-1].casefold()
            if plain_name in target_names:
                result["checks"]["unique_conflict"] = {
                    "risk": "warning",
                    "message": "目标端已存在同名表：若目标表已有数据，主键/唯一键插入可能冲突，建议迁移前清空目标表或核对唯一键数据",
                }
            else:
                result["checks"]["unique_conflict"] = None
        except Exception as exc:  # noqa: BLE001
            notes.append({"section": "quality.unique_conflict", "message": f"表 {table_name} 唯一冲突预判失败：{type(exc).__name__}: {str(exc)[:200]}"})
    except Exception as exc:  # noqa: BLE001 - whole-table degradation
        notes.append({"section": "quality.table", "message": f"表 {table_name} 预检整体失败：{type(exc).__name__}: {str(exc)[:200]}"})
        result["risks"].append({"level": "warning", "code": "QUALITY_FAILED", "message": "预检失败，详见降级说明"})
    return result


def _collect_data_quality(source_config: ConnectionConfig, target_config: ConnectionConfig, notes: list[dict]) -> dict[str, Any]:
    """Lightweight data quality pre-check on source's top tables. All failures degrade to null."""
    result: dict[str, Any] = {"scope": "lightweight", "checked_count": 0, "tables": []}
    source_engine = make_engine(source_config)
    target_names: set[str] = set()
    try:
        target_engine = make_engine(target_config)
        try:
            target_inspector = inspect(target_engine)
            target_schema = default_schema(target_config)
            target_names = {n.casefold() for n in target_inspector.get_table_names(schema=target_schema)}
        finally:
            target_engine.dispose()
    except Exception as exc:  # noqa: BLE001 - conflict pre-check only degrades
        notes.append({"section": "quality.target", "message": f"目标端元数据读取失败，唯一冲突预判跳过：{type(exc).__name__}: {str(exc)[:200]}"})

    try:
        candidates = _quality_candidate_tables(source_engine, source_config, notes)
        if not candidates:
            return result
        source_schema = default_schema(source_config)
        for cand in candidates:
            name = cand["name"]
            schema_use, name_use = source_schema, str(name)
            if "." in str(name):
                schema_use, name_use = str(name).split(".", 1)
            check = _quality_check_table(source_engine, source_config, name_use, schema_use, target_names, notes)
            check["rows_estimate"] = cand["rows_estimate"]
            # build risk items from checks
            for item in check["checks"]["null_rate"]:
                check["risks"].append({
                    "level": "warning", "code": "QUALITY_NULL_RATE",
                    "message": f"列 {item['column']} 空值率约 {int(item['null_rate'] * 100)}%，超过 30% 阈值，迁移后建议核对可空性/默认值",
                })
            dup = check["checks"]["duplicates"]
            if isinstance(dup, dict) and dup.get("exists"):
                check["risks"].append({
                    "level": "warning", "code": "QUALITY_DUPLICATES",
                    "message": f"按唯一键 {dup.get('key')}（{'、'.join(dup.get('columns') or [])}）检测到重复记录，迁移后唯一约束可能创建失败",
                })
            for item in check["checks"]["overlong"]:
                check["risks"].append({
                    "level": "warning", "code": "QUALITY_OVERLONG",
                    "message": f"字符列 {item['column']} 源长度 {item['source_length']} 接近/超过目标端上限（{item['target_type']}），存在截断风险",
                })
            for item in check["checks"]["encoding"]:
                check["risks"].append({
                    "level": "warning", "code": "QUALITY_ENCODING",
                    "message": f"列 {item['column']} 采样发现{item['issue']}，迁移后可能出现乱码或校验失败",
                })
            conflict = check["checks"]["unique_conflict"]
            if isinstance(conflict, dict):
                check["risks"].append({"level": "warning", "code": "QUALITY_UNIQUE_CONFLICT", "message": conflict["message"]})
            result["tables"].append(check)
        result["checked_count"] = len(result["tables"])
    finally:
        source_engine.dispose()
    return result


# ---------------------------------------------------------------------------
# comparison, risks, suggestions
# ---------------------------------------------------------------------------

_PARAM_DEFINITIONS = (
    ("version", "数据库版本", "实例与环境", "version"),
    ("charset", "字符集", "字符集", "charset"),
    ("collation", "排序规则 / 大小写敏感性", "字符集", "collation"),
    ("memory", "内存参数（SGA/PGA、buffer pool、shared_buffers）", "内存", "memory"),
    ("timezone", "时区", "环境", "timezone"),
    ("connections", "连接数上限", "并发", "connections"),
    ("log_mode", "日志模式（ARCHIVELOG / binlog / wal_level）", "备份与增量", "log_mode"),
    ("sql_mode", "SQL 模式（MySQL sql_mode）", "兼容性", "sql_mode"),
    ("isolation", "默认隔离级别", "事务", "isolation"),
    ("lower_case_tables", "表名大小写敏感（lower_case_table_names）", "兼容性", "lower_case_tables"),
)


def _param_of(env: dict, key: str) -> str | None:
    params = env.get("parameters") or {}
    aliases = {
        "version": ("VERSION",),
        "charset": (),
        "collation": (),
        "memory": (),
        "timezone": (),
        "connections": (),
        "log_mode": (),
        "sql_mode": (),
        "isolation": (),
        "lower_case_tables": (),
    }
    lookup = aliases.get(key, ())
    for name in lookup:
        if name in params and params[name] is not None:
            return str(params[name])
    return None


def _source_param(env: dict, *names: str) -> str | None:
    params = env.get("parameters") or {}
    for name in names:
        value = params.get(name)
        if value is not None:
            return str(value)
    return None


def _build_comparison(source_env: dict, target_env: dict) -> list[dict]:
    source_dialect = source_env.get("dialect")
    target_dialect = target_env.get("dialect")

    def s(*names: str) -> str | None:
        return _source_param(source_env, *names)

    def t(*names: str) -> str | None:
        return _source_param(target_env, *names)

    items: list[dict] = []

    def add(key, label, category, source_value, target_value, risk=None, comment=None):
        items.append({
            "key": key,
            "label": label,
            "category": category,
            "source": source_value,
            "target": target_value,
            "risk": risk,
            "comment": comment,
        })

    # version
    s_ver = source_env.get("version")
    t_ver = target_env.get("version")
    ver_comment = None
    ver_risk = None
    if s_ver and t_ver:
        s_short = s_ver.split()[0] if s_ver else s_ver
        t_short = t_ver.split()[0] if t_ver else t_ver
        if s_short != t_short:
            ver_risk = "info"
            ver_comment = f"源端与目标端版本不一致（{s_short} vs {t_short}），建议先在小数据量上验证兼容性"
    add("version", "数据库版本", "实例与环境", s_ver, t_ver, ver_risk, ver_comment)

    # charset
    s_charset = source_env.get("charset")
    t_charset = target_env.get("charset")
    charset_risk = None
    charset_comment = None
    if s_charset and t_charset and s_charset != t_charset:
        if s_charset.upper().replace("-", "") == "ZHS16GBK" or t_charset.upper().replace("-", "") == "ZHS16GBK":
            charset_risk = "warning"
            charset_comment = "涉及 GBK 与多字节字符集互转，中文字符存在截断/乱码风险，迁移后需重点校验"
        else:
            charset_risk = "warning"
            charset_comment = "字符集不一致，需确认双方可表示相同字符集（UTF-8 族通常兼容）"
    add("charset", "字符集", "字符集", s_charset, t_charset, charset_risk, charset_comment)

    # collation
    s_coll = source_env.get("collation") or _source_param(source_env, "NLS_COMP", "NLS_SORT", "COLLATION_SERVER", "COLLATION_DATABASE", "LC_COLLATE")
    t_coll = target_env.get("collation") or _source_param(target_env, "NLS_COMP", "NLS_SORT", "COLLATION_SERVER", "COLLATION_DATABASE", "LC_COLLATE")
    if source_dialect == "oracle":
        nls_comp = s("NLS_COMP")
        nls_sort = s("NLS_SORT")
        s_coll = f"COMP={nls_comp}, SORT={nls_sort}" if (nls_comp or nls_sort) else s_coll
    coll_risk = None
    coll_comment = None
    if s_coll and t_coll and s_coll != t_coll:
        coll_risk = "info"
        coll_comment = "排序规则不同，影响 ORDER BY / 唯一约束排序语义，迁移后建议复核排序行为"
    add("collation", "排序规则 / 大小写敏感性", "字符集", s_coll, t_coll, coll_risk, coll_comment)

    # lower_case_table_names
    s_lct = s("LOWER_CASE_TABLE_NAMES")
    t_lct = t("LOWER_CASE_TABLE_NAMES")
    lct_risk = None
    lct_comment = None
    if s_lct is not None and t_lct is not None and s_lct != t_lct:
        if s_lct == "0" and t_lct == "1":
            lct_risk = "blocking"
            lct_comment = "源端区分表名大小写而目标端不区分，迁移后表/列名大小写可能丢失或冲突，需先统一命名"
        else:
            lct_risk = "warning"
            lct_comment = "表名大小写敏感性不一致，迁移后需确认对象名大小写"
    if s_lct is not None and t_lct is not None and s_lct == t_lct:
        lct_comment = f"双方均为 {s_lct}（0=区分，1=不区分）"
    add("lower_case_tables", "表名大小写敏感（MySQL lower_case_table_names）", "兼容性", s_lct, t_lct, lct_risk, lct_comment)

    # memory
    if source_dialect == "oracle":
        s_mem = f"SGA={s('SGA_TARGET') or s('SGA_MAX_SIZE')}, PGA={s('PGA_AGGREGATE_TARGET')}"
    elif source_dialect == "mysql":
        s_mem = f"innodb_buffer_pool={s('INNODB_BUFFER_POOL_SIZE')}"
    else:
        s_mem = f"shared_buffers={s('SHARED_BUFFERS')}, work_mem={s('WORK_MEM')}"
    if target_dialect == "oracle":
        t_mem = f"SGA={t('SGA_TARGET') or t('SGA_MAX_SIZE')}, PGA={t('PGA_AGGREGATE_TARGET')}"
    elif target_dialect == "mysql":
        t_mem = f"innodb_buffer_pool={t('INNODB_BUFFER_POOL_SIZE')}"
    else:
        t_mem = f"shared_buffers={t('SHARED_BUFFERS')}, work_mem={t('WORK_MEM')}"
    add("memory", "内存参数（SGA/PGA、buffer pool、shared_buffers）", "内存", s_mem, t_mem, None, "目标端内存参数影响迁移后性能，建议按数据量与并发调整")

    # timezone
    s_tz = s("NLS_TIMEZONE", "TIME_ZONE", "SYSTEM_TIME_ZONE")
    t_tz = t("NLS_TIMEZONE", "TIME_ZONE", "SYSTEM_TIME_ZONE")
    tz_risk = None
    tz_comment = None
    if s_tz and t_tz and s_tz != t_tz:
        tz_risk = "warning"
        tz_comment = "时区不一致，TIMESTAMP WITH TIME ZONE 字段迁移后数值可能偏移"
    add("timezone", "时区", "环境", s_tz, t_tz, tz_risk, tz_comment)

    # connections
    s_conn = s("PROCESSES", "SESSIONS", "MAX_CONNECTIONS")
    t_conn = t("PROCESSES", "SESSIONS", "MAX_CONNECTIONS")
    add("connections", "连接数上限", "并发", s_conn, t_conn, None, None)

    # log mode
    if source_dialect == "oracle":
        s_log = f"log_mode={source_env.get('log_mode')}"
    elif source_dialect == "mysql":
        s_log = f"log_bin={s('LOG_BIN')}, binlog_format={s('BINLOG_FORMAT')}"
    else:
        s_log = f"wal_level={s('WAL_LEVEL')}"
    if target_dialect == "oracle":
        t_log = f"log_mode={target_env.get('log_mode')}"
    elif target_dialect == "mysql":
        t_log = f"log_bin={t('LOG_BIN')}, binlog_format={t('BINLOG_FORMAT')}"
    else:
        t_log = f"wal_level={t('WAL_LEVEL')}"
    log_risk = None
    log_comment = None
    if source_dialect == "oracle" and source_env.get("log_mode") not in (None, "ARCHIVELOG"):
        log_risk = "warning"
        log_comment = "源端未开启 ARCHIVELOG，迁移期间无归档保护，建议迁移窗口内谨慎操作并保留备份"
    if source_dialect == "mysql" and s("LOG_BIN") not in (None, "OFF", "0"):
        log_risk = log_risk or "info"
        log_comment = (log_comment or "") + " 源端已开启 binlog，具备增量同步基础（当前引擎仍为全量）"
    add("log_mode", "日志模式（ARCHIVELOG / binlog / wal_level）", "备份与增量", s_log, t_log, log_risk, log_comment)

    # sql_mode
    s_sql_mode = s("SQL_MODE")
    t_sql_mode = t("SQL_MODE")
    sql_mode_risk = None
    sql_mode_comment = None
    if source_dialect == "mysql" and s_sql_mode and t_sql_mode and s_sql_mode != t_sql_mode:
        sql_mode_risk = "info"
        sql_mode_comment = "MySQL sql_mode 不同，STRICT/ONLY_FULL_GROUP_BY 等差异可能影响写入行为"
    add("sql_mode", "SQL 模式（MySQL sql_mode）", "兼容性", s_sql_mode if source_dialect == "mysql" else None, t_sql_mode if target_dialect == "mysql" else None, sql_mode_risk, sql_mode_comment)

    # isolation
    s_iso = s("TRANSACTION_ISOLATION", "DEFAULT_TRANSACTION_ISOLATION", "SERIALIZABLE")
    t_iso = t("TRANSACTION_ISOLATION", "DEFAULT_TRANSACTION_ISOLATION", "SERIALIZABLE")
    iso_risk = None
    if source_dialect == "oracle":
        s_iso = s("SERIALIZABLE") if s("SERIALIZABLE") == "TRUE" else "READ COMMITTED（默认）"
    if target_dialect == "oracle":
        t_iso = t("SERIALIZABLE") if t("SERIALIZABLE") == "TRUE" else "READ COMMITTED（默认）"
    add("isolation", "默认隔离级别", "事务", s_iso, t_iso, iso_risk, "隔离级别差异不影响全量迁移数据本身，但影响应用读写行为")

    return items


def _build_risks(
    source_env, target_env, source_objects, target_objects, source_data, target_data,
    source_fk, target_fk, comparison,
) -> list[dict]:
    risks: list[dict] = []
    for item in comparison:
        if item.get("risk"):
            risks.append({
                "level": item["risk"],
                "category": item["label"],
                "message": item.get("comment") or f"{item['label']} 源端与目标端不一致",
            })

    def obj_count(objects: dict, key: str) -> int | None:
        return objects.get("counts", {}).get(key)

    for side, objects, data in (("源端", source_objects, source_data), ("目标端", target_objects, target_data)):
        prefix = f"{side}"
        counts = objects.get("counts", {})
        unsupported = {
            "sequences": "序列",
            "synonyms": "同义词",
            "dblinks": "数据库链接",
            "procedures": "存储过程",
            "functions": "函数",
            "packages": "包",
            "triggers": "触发器",
            "materialized_views": "物化视图",
            "scheduler_jobs": "定时任务",
        }
        present = [label for key, label in unsupported.items() if counts.get(key) not in (None, 0)]
        if present:
            risks.append({
                "level": "warning",
                "category": f"{prefix}对象",
                "message": f"{prefix}存在 {len(present)} 类当前引擎不支持迁移的对象（{'、'.join(present)}），需人工处理",
            })
        no_pk = data.get("no_pk_tables")
        if isinstance(no_pk, list) and no_pk:
            risks.append({
                "level": "warning",
                "category": f"{prefix}数据质量",
                "message": f"{prefix}有 {len(no_pk)} 张无主键表（如 {'、'.join(no_pk[:3])}{'…' if len(no_pk) > 3 else ''}），无法做行级哈希校验；目标 MySQL 将自动补自增主键",
            })
        lob = data.get("lob_tables")
        if isinstance(lob, list) and lob:
            risks.append({
                "level": "warning",
                "category": f"{prefix}大字段",
                "message": f"{prefix}有 {len(lob)} 张表含 LOB/大字段，迁移耗时与目标容量需评估，迁移后建议重点校验大对象完整性",
            })
        total_rows = data.get("total_rows_estimate")
        if total_rows is not None and total_rows > 50_000_000:
            risks.append({
                "level": "warning",
                "category": f"{prefix}数据量",
                "message": f"{prefix}估算行数约 {total_rows:,}，全量迁移耗时长，建议规划停机窗口或分批迁移",
            })

    fk_count = source_fk.get("count")
    if fk_count is not None and fk_count > 0:
        risks.append({
            "level": "info",
            "category": "外键依赖",
            "message": f"源端有 {fk_count} 个外键约束，当前引擎不迁移外键；迁移后需在目标端重建约束，注意父子表迁移顺序",
        })
    return risks


def _build_suggestions(source_env, target_env, source_objects, source_data, source_fk, risks) -> list[str]:
    suggestions: list[str] = []
    src_dialect = source_env.get("dialect")
    total_bytes = source_data.get("total_bytes")
    total_rows = source_data.get("total_rows_estimate")
    if total_bytes is not None and total_bytes > 10 * 1024 ** 3:
        suggestions.append("源端数据量超过 10 GB，建议：先小表演练 → 分阶段迁移大表 → 迁移期间暂停业务写入或使用维护窗口")
    elif total_rows is not None and total_rows > 5_000_000:
        suggestions.append("源端行数较多，建议使用较大的 batch_size 与表并发，并在非高峰时段执行")
    if total_rows is not None and total_rows > 0:
        suggestions.append("当前引擎为全量迁移（无增量 CDC）。若需要低停机迁移，需在业务侧规划增量同步或切换窗口")
    lob = source_data.get("lob_tables")
    if isinstance(lob, list) and lob:
        suggestions.append(f"源端含 {len(lob)} 张 LOB 表：迁移后逐一校验大对象（长度/哈希），并确认目标端可存储最大 LOB 尺寸")
    no_pk = source_data.get("no_pk_tables")
    if isinstance(no_pk, list) and no_pk:
        suggestions.append(f"源端 {len(no_pk)} 张无主键表：建议迁移前补充主键/唯一键，否则目标端校验将降级为仅行数对比")
    counts = source_objects.get("counts", {})
    manual_objects = [
        (counts.get("procedures"), "存储过程"),
        (counts.get("functions"), "函数"),
        (counts.get("packages"), "包"),
        (counts.get("triggers"), "触发器"),
        (counts.get("sequences"), "序列"),
        (counts.get("synonyms"), "同义词"),
        (counts.get("dblinks"), "数据库链接"),
        (counts.get("materialized_views"), "物化视图"),
        (counts.get("scheduler_jobs"), "定时任务"),
    ]
    manual = [label for count, label in manual_objects if count not in (None, 0)]
    if manual:
        suggestions.append(f"以下对象需人工迁移：{'、'.join(manual)}（当前引擎只迁移表与视图）")
    if source_fk.get("count"):
        suggestions.append("外键约束需在数据迁移完成后在目标端手工重建，迁移顺序建议父表先行")
    if source_env.get("charset") and target_env.get("charset") and source_env["charset"] != target_env["charset"]:
        suggestions.append("字符集不一致：迁移后用代表性数据（中文、emoji、特殊符号）做抽样校验")
    if any(risk["level"] == "blocking" for risk in risks):
        suggestions.append("存在阻断项，请先处理阻断项后再发起迁移")
    if not suggestions:
        suggestions.append("未发现明显迁移风险，可按标准流程执行全量迁移并在完成后运行迁移校验")
    return suggestions


# ---------------------------------------------------------------------------
# P2: migration time estimation
# ---------------------------------------------------------------------------


def _human_duration(seconds: float | None) -> str | None:
    """Render a duration estimate as a human readable string (估算值)."""
    if seconds is None:
        return None
    total = max(0, int(round(seconds)))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours} 小时 {minutes} 分"
    if minutes > 0:
        return f"{minutes} 分 {secs} 秒"
    return f"{secs} 秒"


def _estimate_migration_time(
    source_config: ConnectionConfig,
    source_data: dict[str, Any],
    bandwidth_mbps: float = 50.0,
    batch_size: int = 2000,
    table_concurrency: int = 1,
) -> dict[str, Any]:
    """Build a per-table + summary migration duration estimate (估算模型).

    All numbers are estimates based on configurable assumptions; every figure
    is explicitly labelled as an estimate so users do not mistake them for
    measured values.
    """
    concurrency = table_concurrency
    top = source_data.get("top_tables")
    if not isinstance(top, list) or not top:
        return {
            "available": False,
            "bandwidth_mbps": bandwidth_mbps,
            "batch_size": batch_size,
            "table_concurrency": concurrency,
            "assumptions": ["缺少 TOP 表数据（源端信息不可用），无法进行耗时估算"],
            "per_table": [],
            "summary": None,
        }
    assumptions = [
        f"网络带宽按 {bandwidth_mbps} Mbps 估算（可配置，默认值；实际请按生产网络调整）",
        f"传输耗时 = 表容量 × 8 ÷ 带宽，仅计数据网络传输，未计协议/压缩开销",
        f"复制耗时按经验速率：每批 {batch_size} 行，单批固定 0.05s，每行额外 0.00002s",
        "乐观场景取经验速率 × 0.8，悲观场景取 × 1.6",
        f"表并发 {concurrency}：汇总耗时按并发均摊（假设表间无资源争抢，实际受 CPU/IO 影响）",
        "以上均为估算值，实际耗时受网络质量、硬件、目标端写入性能影响",
    ]
    per_table: list[dict[str, Any]] = []
    total_opt = 0.0
    total_pess = 0.0
    has_copy = False
    for table in top[:50]:
        name = str(table.get("table") or "?")
        rows = table.get("rows_estimate")
        size = table.get("size_bytes")
        transfer = (size * 8 / (bandwidth_mbps * 1_000_000)) if size not in (None, 0) else 0.0
        if rows not in (None, 0):
            batches = max(1, int(rows / batch_size))
            copy_base = batches * 0.05 + rows * 0.00002
            copy_opt = round(copy_base * 0.8, 1)
            copy_pess = round(copy_base * 1.6, 1)
            has_copy = True
        else:
            copy_opt = copy_pess = None
        table_opt = round(transfer + (copy_opt or 0.0), 1)
        table_pess = round(transfer + (copy_pess or 0.0), 1)
        total_opt += table_opt
        total_pess += table_pess
        per_table.append({
            "table": name,
            "rows_estimate": rows,
            "size_bytes": size,
            "transfer_seconds": round(transfer, 1),
            "copy_seconds_optimistic": copy_opt,
            "copy_seconds_pessimistic": copy_pess,
            "total_optimistic": table_opt,
            "total_pessimistic": table_pess,
        })
    wall_opt = round(total_opt / concurrency, 1)
    wall_pess = round(total_pess / concurrency, 1)
    summary = {
        "total_optimistic_seconds": wall_opt,
        "total_pessimistic_seconds": wall_pess,
        "total_optimistic": _human_duration(wall_opt),
        "total_pessimistic": _human_duration(wall_pess),
        "rows_covered": sum(1 for t in per_table if t["rows_estimate"] not in (None, 0)),
        "tables_covered": len(per_table),
        "has_copy_estimate": has_copy,
    }
    return {
        "available": True,
        "bandwidth_mbps": bandwidth_mbps,
        "batch_size": batch_size,
        "table_concurrency": concurrency,
        "assumptions": assumptions,
        "per_table": per_table,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# P2: automatic conclusion generation (dimensions + action items)
# ---------------------------------------------------------------------------


def _dimension_action(priority: str, owner: str, description: str) -> dict[str, str]:
    return {"priority": priority, "owner": owner, "description": description}


def _build_conclusion(
    comparison: list[dict[str, Any]],
    source_objects: dict[str, Any],
    source_data: dict[str, Any],
    source_fk: dict[str, Any],
    data_quality: dict[str, Any],
    risks: list[dict[str, Any]],
    score: int,
    ready: bool,
    blocking: int,
) -> dict[str, Any]:
    """Auto-generate a structured migration conclusion from collected data."""
    # dimension: instance parameters
    param_bad = [c for c in comparison if c.get("level") in ("warning", "blocking")]
    param_level = "blocking" if any(c.get("level") == "blocking" for c in param_bad) else ("warning" if param_bad else "ok")
    param_actions: list[dict[str, str]] = []
    if param_level != "ok":
        for c in param_bad[:8]:
            param_actions.append(_dimension_action(
                "P0" if c.get("level") == "blocking" else "P1",
                "DBA",
                f"对齐参数 {c.get('name') or c.get('key') or '?'}：源 {c.get('source')} → 目标 {c.get('target')}",
            ))
    else:
        param_actions.append(_dimension_action("P2", "DBA", "实例关键参数差异较小，无需特殊处理"))

    # dimension: objects
    counts = source_objects.get("counts", {})
    manual_objs = [
        (counts.get(k), label)
        for k, label in (
            ("procedures", "存储过程"), ("functions", "函数"), ("packages", "包"),
            ("triggers", "触发器"), ("sequences", "序列"), ("synonyms", "同义词"),
            ("dblinks", "数据库链接"), ("materialized_views", "物化视图"), ("scheduler_jobs", "定时任务"),
        )
    ]
    manual = [label for count, label in manual_objs if count not in (None, 0)]
    obj_level = "warning" if manual else "ok"
    obj_actions: list[dict[str, str]] = []
    if manual:
        obj_actions.append(_dimension_action(
            "P1", "DBA", f"以下对象需人工迁移：{'、'.join(manual)}（当前引擎只迁移表与视图）",
        ))
    else:
        obj_actions.append(_dimension_action("P2", "DBA", "无需要人工迁移的数据库对象"))

    # dimension: data
    data_level = "ok"
    data_actions: list[dict[str, str]] = []
    no_pk = source_data.get("no_pk_tables")
    lob = source_data.get("lob_tables")
    if isinstance(no_pk, list) and no_pk:
        data_level = "warning"
        data_actions.append(_dimension_action("P1", "开发", f"{len(no_pk)} 张无主键表，建议迁移前补充主键/唯一键"))
    if isinstance(lob, list) and lob:
        data_level = "warning" if data_level == "ok" else data_level
        data_actions.append(_dimension_action("P2", "DBA", f"{len(lob)} 张 LOB 表，迁移后需逐一校验大对象"))
    if not data_actions:
        data_actions.append(_dimension_action("P2", "开发", "未发现明显数据风险，按标准流程迁移"))

    # dimension: foreign keys
    fk_count = source_fk.get("count")
    fk_level = "warning" if fk_count not in (None, 0) else "ok"
    fk_actions: list[dict[str, str]] = []
    if fk_level == "warning":
        fk_actions.append(_dimension_action("P1", "DBA", f"共 {fk_count} 条外键：数据迁移完成后在目标端手工重建，父表先行"))
    else:
        fk_actions.append(_dimension_action("P2", "DBA", "无外键依赖或外键信息不可用"))

    # dimension: data quality
    quality_risks = [r for r in risks if str(r.get("category", "")).startswith("数据质量")]
    quality_level = "blocking" if any(r.get("level") == "blocking" for r in quality_risks) else (
        "warning" if quality_risks else "ok"
    )
    quality_actions: list[dict[str, str]] = []
    if quality_risks:
        for r in quality_risks[:8]:
            quality_actions.append(_dimension_action(
                "P0" if r.get("level") == "blocking" else "P1",
                "开发",
                f"{r.get('category')}：{r.get('message')}",
            ))
    else:
        quality_actions.append(_dimension_action("P2", "开发", "数据质量预检未发现明显风险（或信息不可用）"))

    if blocking > 0:
        overall_statement = f"存在 {blocking} 个阻断项，当前不建议发起迁移；请先处理阻断项（P0）后重新评估"
    elif ready:
        overall_statement = f"整体就绪（评分 {score}/100），可按计划发起迁移；建议按下方待办清单执行迁移准备"
    else:
        overall_statement = f"整体评估评分 {score}/100，存在需关注的风险，建议处理 P1 待办后再次评估"

    dimensions = [
        {"name": "实例参数", "level": param_level,
         "summary": f"{len(param_bad)} 项参数差异需关注" if param_bad else "关键参数差异较小",
         "action_items": param_actions},
        {"name": "对象", "level": obj_level,
         "summary": f"{len(manual)} 类对象需人工迁移" if manual else "对象迁移以表/视图为主，无额外人工项",
         "action_items": obj_actions},
        {"name": "数据", "level": data_level,
         "summary": f"无主键表 {len(no_pk) if isinstance(no_pk, list) else '未知'} 张、LOB 表 {len(lob) if isinstance(lob, list) else '未知'} 张",
         "action_items": data_actions},
        {"name": "外键", "level": fk_level,
         "summary": f"外键 {fk_count if fk_count is not None else '未知'} 条，需迁移后重建",
         "action_items": fk_actions},
        {"name": "数据质量", "level": quality_level,
         "summary": f"{len(quality_risks)} 项质量风险" if quality_risks else "未发现明显质量风险",
         "action_items": quality_actions},
    ]
    return {
        "overall": {
            "ready": ready,
            "score": score,
            "statement": overall_statement,
        },
        "dimensions": dimensions,
    }


# ---------------------------------------------------------------------------
# P2: permission / security inventory
# ---------------------------------------------------------------------------


def _collect_security(config: ConnectionConfig, notes: list[dict]) -> dict[str, Any]:
    """Collect account / role / privilege + security settings summary (best effort)."""
    engine = make_engine(config)
    result: dict[str, Any] = {"dialect": config.type, "accounts": None, "roles": None,
                              "system_privileges": None, "sensitive_accounts": None,
                              "security_settings": None}
    try:
        probe_sql = "SELECT 1 FROM DUAL" if config.type == "oracle" else "SELECT 1"
        with engine.connect() as conn:
            conn.execute(text(probe_sql))
        if config.type == "oracle":
            users = _fetch(engine, "SELECT username, account_status FROM dba_users ORDER BY username FETCH FIRST 101 ROWS ONLY", notes, "oracle.security.users")
            users_total = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM dba_users", notes, "oracle.security.users_total")
            if users is not None:
                result["accounts"] = _truncated_detail([{"account": str(u.get("username")), "status": str(u.get("account_status"))} for u in users], users_total)
            roles = _fetch(engine, "SELECT granted_role, COUNT(*) AS cnt FROM dba_role_privs GROUP BY granted_role ORDER BY COUNT(*) DESC FETCH FIRST 21 ROWS ONLY", notes, "oracle.security.roles")
            if roles is not None:
                result["roles"] = _truncated_detail([{"role": str(r.get("granted_role")), "grants": _int(r.get("cnt"))} for r in roles], None)
            sys_privs = _fetch(engine, "SELECT privilege, COUNT(*) AS cnt FROM dba_sys_privs GROUP BY privilege ORDER BY COUNT(*) DESC FETCH FIRST 21 ROWS ONLY", notes, "oracle.security.sys_privs")
            if sys_privs is not None:
                result["system_privileges"] = _truncated_detail([{"privilege": str(p.get("privilege")), "grants": _int(p.get("cnt"))} for p in sys_privs], None)
            sensitive = ["SYS", "SYSTEM", "DBSNMP", "OUTLN", "XDB", "SYSMAN", "MGMT_VIEW"]
            if users is not None:
                found_sensitive = [str(u.get("username")) for u in users if str(u.get("username")).upper() in sensitive]
                result["sensitive_accounts"] = found_sensitive
            tde_count = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM dba_encrypted_tablespaces", notes, "oracle.security.tde")
            audit_trail = _fetch_scalar(engine, "SELECT value FROM v$parameter WHERE name='audit_trail'", notes, "oracle.security.audit")
            settings: dict[str, Any] = {}
            if tde_count is not None:
                settings["tde_tablespaces"] = int(tde_count)
            if audit_trail is not None:
                settings["audit_trail"] = str(audit_trail)
            result["security_settings"] = settings if settings else None
            if not settings:
                notes.append({"section": "oracle.security.settings", "message": "TDE/审计信息不可查（无权限），安全设置项为空"})
        elif config.type == "mysql":
            users = _fetch(engine, "SELECT User, Host, plugin FROM mysql.user ORDER BY User LIMIT 101", notes, "mysql.security.users")
            users_total = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM mysql.user", notes, "mysql.security.users_total")
            if users is not None:
                result["accounts"] = _truncated_detail([{"account": str(u.get("User")), "host": str(u.get("Host")), "plugin": str(u.get("plugin"))} for u in users], users_total)
            sys_privs = _fetch(engine, "SELECT DISTINCT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES ORDER BY PRIVILEGE_TYPE LIMIT 101", notes, "mysql.security.privs")
            if sys_privs is not None:
                result["system_privileges"] = _truncated_detail([{"privilege": str(p.get("PRIVILEGE_TYPE"))} for p in sys_privs], None)
            if users is not None:
                found_sensitive = [str(u.get("User")) for u in users if str(u.get("User")).lower() in ("root", "admin", "administrator")]
                result["sensitive_accounts"] = found_sensitive
            settings: dict[str, Any] = {}
            ssl = _fetch(engine, "SELECT User, ssl_type FROM mysql.user WHERE ssl_type <> '' LIMIT 21", notes, "mysql.security.ssl")
            if ssl is not None:
                settings["ssl_accounts"] = len(ssl)
            result["security_settings"] = settings if settings else None
            notes.append({"section": "mysql.security.roles", "message": "MySQL 无独立角色对象清单（角色等价于账号），roles 置 null"})
        else:  # postgresql
            roles = _fetch(engine, "SELECT rolname, rolsuper, rolcanlogin FROM pg_roles WHERE rolname NOT LIKE 'pg_%' ORDER BY rolname LIMIT 101", notes, "pg.security.roles")
            if roles is not None:
                result["roles"] = _truncated_detail([{"role": str(r.get("rolname")), "superuser": bool(r.get("rolsuper")), "can_login": bool(r.get("rolcanlogin"))} for r in roles], len(roles))
            super_count = _fetch_scalar(engine, "SELECT COUNT(*) AS c FROM pg_roles WHERE rolsuper AND rolname NOT LIKE 'pg_%'", notes, "pg.security.super_count")
            settings: dict[str, Any] = {}
            if super_count is not None:
                settings["superuser_count"] = int(super_count)
            result["security_settings"] = settings if settings else None
            notes.append({"section": "pg.security.privs", "message": "PostgreSQL 无跨角色系统权限汇总视图，system_privileges 置 null"})
            result["system_privileges"] = None
    except Exception as exc:  # noqa: BLE001
        notes.append({"section": "security", "message": f"权限与安全清单采集失败：{type(exc).__name__}: {str(exc)[:300]}"})
    finally:
        engine.dispose()
    return result


# ---------------------------------------------------------------------------
# P2: performance / load pressure assessment
# ---------------------------------------------------------------------------


def _performance_pressure(source_env: dict[str, Any], source_data: dict[str, Any], source_config: ConnectionConfig) -> dict[str, Any]:
    host = source_env.get("host_resources") or {}
    cpu_cores = host.get("cpu_cores")
    top = source_data.get("top_tables")
    max_bytes = max((t.get("size_bytes") or 0) for t in top) if isinstance(top, list) and top else None
    max_rows = max((t.get("rows_estimate") or 0) for t in top) if isinstance(top, list) and top else None

    level = "低"
    if max_bytes is not None and max_bytes > 50 * 1024 ** 3:
        level = "高"
    elif max_bytes is not None and max_bytes > 10 * 1024 ** 3:
        level = "中"
    elif max_rows is not None and max_rows > 50_000_000:
        level = "高"
    elif max_rows is not None and max_rows > 10_000_000:
        level = "中"

    recommended = min(16, max(1, int(cpu_cores or 4)))
    rationale_parts = []
    if max_bytes is not None:
        rationale_parts.append(f"最大表容量约 {max_bytes / 1024 ** 3:.1f} GB")
    if max_rows is not None:
        rationale_parts.append(f"最大表估算行数约 {max_rows:,}")
    if cpu_cores is not None:
        rationale_parts.append(f"源端 CPU {cpu_cores} 核")
    rationale = ("；".join(rationale_parts) + "；" if rationale_parts else "") + (
        "全量并发读取会对源端产生负载，等级为" + level + "，" + f"建议表并发上限 {recommended}"
    )

    return {
        "level": level,
        "source_cpu_impact": "高" if level == "高" else ("中" if level == "中" else "低"),
        "source_io_impact": "高" if level == "高" else ("中" if level == "中" else "低"),
        "max_table_bytes": max_bytes,
        "max_table_rows": max_rows,
        "source_cpu_cores": cpu_cores,
        "recommended_table_concurrency": recommended,
        "low_peak_advice": "建议在业务低峰时段执行迁移，避免与生产高峰读写叠加",
        "rationale": rationale,
    }


# ---------------------------------------------------------------------------
# P2: markdown report export
# ---------------------------------------------------------------------------


def _md_escape(value: Any) -> str:
    if value is None:
        return "—"
    text_value = str(value)
    return text_value.replace("|", "\\|").replace("\n", " ")


def _report_section(title: str, body: str) -> str:
    return f"\n## {title}\n\n{body}"


def _html_escape(value: Any) -> str:
    if value is None:
        return "—"
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _html_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return "<p class=\"muted\">无记录</p>"
    head = "".join(f"<th>{_html_escape(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{_html_escape(c)}</td>" for c in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def export_deep_report(payload: dict[str, Any], output_dir: str | Path) -> dict[str, str]:
    """Render the full deep assessment payload as a self-contained HTML report."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    file_name = f"flowdb_deep_report_{stamp}.html"
    file_path = out_path / file_name

    src = payload.get("source", {})
    tgt = payload.get("target", {})
    src_env = src.get("env", {})
    tgt_env = tgt.get("env", {})
    score = payload.get("score")
    ready = payload.get("ready")

    sections: list[str] = []

    # header
    ready_badge = '<span class="ok">是</span>' if ready else '<span class="bad">否</span>'
    sections.append(
        '<div class="card header">'
        "<h1>FlowDB 深度迁移前评估报告</h1>"
        f"<p>生成时间：{_html_escape(payload.get('generated_at'))}</p>"
        f"<p>源端：{_html_escape(src_env.get('dialect', '?'))} @ {_html_escape(src_env.get('host', '?'))}</p>"
        f"<p>目标端：{_html_escape(tgt_env.get('dialect', '?'))} @ {_html_escape(tgt_env.get('host', '?'))}</p>"
        f'<div class="score">评分 <b>{_html_escape(score)}</b>/100 · 就绪：{ready_badge}</div>'
        "</div>"
    )

    # parameter comparison
    rows = [[c.get("name"), c.get("source"), c.get("target"), c.get("level")] for c in payload.get("parameter_comparison", [])]
    sections.append("<div class=\"card\"><h2>参数对比</h2>" + _html_table(["参数", "源端", "目标端", "级别"], rows) + "</div>")

    # object stats + details
    obj_parts: list[str] = []
    for side_label, side in (("源端", src), ("目标端", tgt)):
        counts = side.get("objects", {}).get("counts", {})
        if counts:
            summary = "，".join(f"{k}={v}" for k, v in counts.items() if v is not None)
            obj_parts.append(f"<h3>{side_label} 对象计数</h3><p>{_html_escape(summary)}</p>")
    details = src.get("objects", {}).get("details")
    if details:
        for key, label in (("sequences", "序列"), ("synonyms", "同义词"), ("dblinks", "DBLINK"),
                           ("procedures", "存储过程/函数"), ("triggers", "触发器")):
            detail = details.get(key)
            if detail and detail.get("items"):
                items = detail["items"]
                head = list(items[0].keys())
                rows = [[it.get(h) for h in head] for it in items]
                obj_parts.append(f"<h3>{label}（{detail.get('total', len(items))} 条）</h3>" + _html_table(head, rows))
    sections.append("<div class=\"card\"><h2>对象规模与明细</h2>" + ("".join(obj_parts) if obj_parts else "<p class=\"muted\">对象信息不可用</p>") + "</div>")

    # data analysis
    da = payload.get("data_analysis", {}).get("source", {})
    data_parts: list[str] = [
        f"<p>总估算容量：{_html_escape(da.get('total_bytes'))} 字节；总估算行数：{_html_escape(da.get('total_rows_estimate'))}；空表数：{_html_escape(da.get('empty_table_count'))}</p>"
    ]
    top = da.get("top_tables")
    if top:
        rows = [[t.get("table"), t.get("rows_estimate"), t.get("size_bytes"), t.get("column_count"),
                 "有" if t.get("has_pk") else "无", "是" if t.get("partitioned") else "否"] for t in top]
        data_parts.append("<h3>TOP 10 大表</h3>" + _html_table(["表", "估算行数", "容量(字节)", "列数", "主键", "分区"], rows))
    no_pk = da.get("no_pk_tables")
    if isinstance(no_pk, list) and no_pk:
        data_parts.append(f"<p>无主键表：{_html_escape('、'.join(str(n) for n in no_pk))}</p>")
    sections.append("<div class=\"card\"><h2>数据量分析</h2>" + "".join(data_parts) + "</div>")

    # foreign keys
    fk_src = payload.get("foreign_keys", {}).get("source", {})
    fk_parts = [f"<p>外键数量：{_html_escape(fk_src.get('count'))}</p>"]
    deps = fk_src.get("dependencies")
    if deps:
        rows = [[d.get("parent_table"), d.get("child_table")] for d in deps]
        fk_parts.append("<h3>依赖表对（父表 → 子表）</h3>" + _html_table(["父表", "子表"], rows))
    sections.append("<div class=\"card\"><h2>外键依赖</h2>" + "".join(fk_parts) + "</div>")

    # data quality
    dq = payload.get("data_quality", {})
    dq_parts: list[str] = []
    for table in dq.get("tables", []):
        checks = table.get("checks", {})
        dq_parts.append(f"<h3>{_html_escape(table.get('table'))}（行数 {_html_escape(table.get('rows'))}）</h3>")
        if checks.get("null_rate"):
            dq_parts.append("<p>高空值率列：" + "、".join(f"{c.get('column')}({c.get('null_rate')})" for c in checks["null_rate"]) + "</p>")
        dup = checks.get("duplicates")
        dq_parts.append("<p>重复记录：" + ("存在" if dup and dup.get("exists") else "未发现") + "</p>")
        if checks.get("overlong"):
            dq_parts.append("<p>超长风险列：" + "、".join(str(c.get("column")) for c in checks["overlong"]) + "</p>")
        if checks.get("encoding"):
            dq_parts.append("<p>编码异常列：" + "、".join(str(c.get("column")) for c in checks["encoding"]) + "</p>")
        if checks.get("unique_conflict"):
            dq_parts.append("<p>目标端唯一冲突预判：存在风险</p>")
    sections.append("<div class=\"card\"><h2>数据质量预检</h2>" + ("".join(dq_parts) if dq_parts else "<p class=\"muted\">数据质量预检不可用</p>") + "</div>")

    # partition analysis
    pa = payload.get("partition_analysis", {})
    pa_parts: list[str] = []
    if pa:
        pa_parts.append(f"<p>分区表总数：{_html_escape(pa.get('partitioned_total'))}</p>")
        by_type = pa.get("by_type") or {}
        if by_type:
            rows = [[k, v] for k, v in by_type.items()]
            pa_parts.append(_html_table(["分区类型", "数量"], rows))
        interval = pa.get("interval_tables") or []
        if interval:
            pa_parts.append(f"<p><b>间隔分区表（{len(interval)} 个）</b>：{_html_escape('、'.join(str(n) for n in interval[:20]))}</p>")
            pa_parts.append("<p class=\"muted\">TDSQL 不支持 Oracle 间隔分区，迁移时将转换为普通 RANGE 分区表</p>")
        else:
            pa_parts.append("<p>未检测到间隔分区表</p>")
        for d in pa.get("downgrades") or []:
            pa_parts.append(f"<p class=\"muted\">降级提示：{_html_escape(str(d))}</p>")
    else:
        pa_parts.append("<p class=\"muted\">分区表分析不可用</p>")
    sections.append("<div class=\"card\"><h2>分区表分析</h2>" + "".join(pa_parts) + "</div>")

    # security
    sec = payload.get("security", {})
    sec_parts: list[str] = []
    for label, key in (("账号", "accounts"), ("角色", "roles"), ("系统权限", "system_privileges")):
        item = sec.get(key)
        if item and item.get("items"):
            items = item["items"]
            head = list(items[0].keys())
            rows = [[r.get(h) for h in head] for r in items]
            sec_parts.append(f"<h3>{label}（{item.get('total', len(items))} 条）</h3>" + _html_table(head, rows))
        else:
            sec_parts.append(f"<p>{label}：不可用</p>")
    if sec.get("sensitive_accounts"):
        sec_parts.append(f"<p>敏感账号：{_html_escape('、'.join(str(s) for s in sec['sensitive_accounts']))}</p>")
    if sec.get("security_settings"):
        sec_parts.append(f"<p>安全设置：{_html_escape(sec['security_settings'])}</p>")
    sections.append("<div class=\"card\"><h2>权限与安全清单</h2>" + ("".join(sec_parts) if sec_parts else "<p class=\"muted\">权限与安全清单不可用</p>") + "</div>")

    # time estimate
    te = payload.get("time_estimate", {})
    te_parts: list[str] = []
    if te.get("assumptions"):
        te_parts.append("<p><b>估算假设（均为估算值）</b></p><ul>" + "".join(f"<li>{_html_escape(a)}</li>" for a in te["assumptions"]) + "</ul>")
    summary = te.get("summary")
    if summary:
        te_parts.append(f"<p><b>汇总</b>：乐观 {_html_escape(summary.get('total_optimistic'))}，悲观 {_html_escape(summary.get('total_pessimistic'))}（覆盖 {_html_escape(summary.get('tables_covered'))} 张 TOP 表）</p>")
    if te.get("per_table"):
        rows = [[t.get("table"), t.get("rows_estimate"), t.get("size_bytes"), t.get("transfer_seconds"),
                 t.get("copy_seconds_optimistic"), t.get("copy_seconds_pessimistic"), t.get("total_optimistic"), t.get("total_pessimistic")] for t in te["per_table"]]
        te_parts.append(_html_table(["表", "估算行数", "容量(字节)", "传输(s)", "复制乐观(s)", "复制悲观(s)", "乐观合计(s)", "悲观合计(s)"], rows))
    sections.append("<div class=\"card\"><h2>迁移耗时估算</h2>" + ("".join(te_parts) if te_parts else "<p class=\"muted\">迁移耗时估算不可用</p>") + "</div>")

    # performance pressure
    perf = payload.get("performance", {})
    perf_parts = [
        f"<p>负载等级：{_html_escape(perf.get('level'))}</p>",
        f"<p>源端 CPU 影响：{_html_escape(perf.get('source_cpu_impact'))}；源端 IO 影响：{_html_escape(perf.get('source_io_impact'))}</p>",
        f"<p>建议表并发上限：{_html_escape(perf.get('recommended_table_concurrency'))}</p>",
        f"<p>{_html_escape(perf.get('low_peak_advice'))}</p>",
        f"<p>依据：{_html_escape(perf.get('rationale'))}</p>",
    ] if perf else ["<p class=\"muted\">性能压力评估不可用</p>"]
    sections.append("<div class=\"card\"><h2>性能压力评估</h2>" + "".join(perf_parts) + "</div>")

    # risks
    risk_rows = [[r.get("level"), r.get("category"), r.get("message")] for r in payload.get("risks", [])]
    sections.append("<div class=\"card\"><h2>风险清单</h2>" + _html_table(["级别", "分类", "说明"], risk_rows) + "</div>")

    # conclusion
    conc = payload.get("conclusion", {})
    conc_parts: list[str] = []
    overall = conc.get("overall", {})
    conc_parts.append(f"<p><b>总体结论</b>：{_html_escape(overall.get('statement'))}</p>")
    for dim in conc.get("dimensions", []):
        conc_parts.append(f"<h3>{_html_escape(dim.get('name'))}（{_html_escape(dim.get('level'))}）</h3>")
        conc_parts.append(f"<p>{_html_escape(dim.get('summary'))}</p>")
        rows = [[a.get("priority"), a.get("owner"), a.get("description")] for a in dim.get("action_items", [])]
        conc_parts.append(_html_table(["优先级", "责任方", "待办"], rows))
    sections.append("<div class=\"card\"><h2>迁移结论与待办</h2>" + ("".join(conc_parts) if conc_parts else "<p class=\"muted\">迁移结论不可用</p>") + "</div>")

    # suggestions + notes
    sug = "".join(f"<li>{_html_escape(s)}</li>" for s in payload.get("suggestions", []))
    sections.append("<div class=\"card\"><h2>迁移建议</h2>" + (f"<ul>{sug}</ul>" if sug else "<p class=\"muted\">无建议</p>") + "</div>")
    note_rows = [[n.get("side"), n.get("section"), n.get("message")] for n in payload.get("notes", [])]
    sections.append("<div class=\"card\"><h2>降级说明</h2>" + (_html_table(["端", "分节", "说明"], note_rows) if note_rows else "<p class=\"muted\">无降级说明</p>") + "</div>")

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>FlowDB 深度迁移前评估报告</title>
<style>
body {{ font-family: -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif; margin: 0; background: #f4f6fa; color: #263044; font-size: 14px; line-height: 1.6; }}
.wrap {{ max-width: 1100px; margin: 0 auto; padding: 28px 20px 60px; }}
.card {{ background: #fff; border: 1px solid #e3e8f0; border-radius: 12px; padding: 22px 26px; margin-bottom: 18px; box-shadow: 0 3px 12px #26354f08; }}
.header {{ background: linear-gradient(135deg, #1d4ed8, #4f46e5); color: #fff; border: 0; }}
.header h1 {{ margin: 0 0 12px; font-size: 24px; }}
.header p {{ margin: 4px 0; opacity: .92; font-size: 13px; }}
.score {{ margin-top: 14px; font-size: 15px; font-weight: 700; }}
.score .ok {{ color: #8ef0b8; }} .score .bad {{ color: #ffb3b3; }}
h2 {{ font-size: 17px; margin: 0 0 14px; color: #1e293b; border-left: 4px solid #4f46e5; padding-left: 10px; }}
h3 {{ font-size: 14px; margin: 18px 0 8px; color: #334155; }}
p {{ margin: 6px 0; }}
ul {{ margin: 6px 0; padding-left: 22px; }}
table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 13px; }}
th {{ background: #f1f5f9; color: #475569; text-align: left; padding: 8px 10px; border: 1px solid #e2e8f0; font-weight: 600; }}
td {{ padding: 7px 10px; border: 1px solid #e8edf4; color: #334155; word-break: break-all; }}
tr:nth-child(even) td {{ background: #fafbfd; }}
.muted {{ color: #94a3b8; }}
@media print {{ body {{ background: #fff; }} .card {{ box-shadow: none; break-inside: avoid; }} }}
</style>
</head>
<body>
<div class="wrap">
{''.join(sections)}
</div>
</body>
</html>
"""
    file_path.write_text(html, encoding="utf-8")
    return {"file_path": str(file_path), "file_name": file_name,
            "download_url": f"/api/reports/{file_name}"}


# ---------------------------------------------------------------------------
# public entry
# ---------------------------------------------------------------------------


def deep_assess_payload(
    source: dict[str, Any],
    target: dict[str, Any],
    bandwidth_mbps: float = 50.0,
    batch_size: int = 2000,
    table_concurrency: int = 1,
    owners: list[str] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    source_config = ConnectionConfig.model_validate(source)
    target_config = ConnectionConfig.model_validate(target)
    if source_config.type not in _COLLECTORS or target_config.type not in _COLLECTORS:
        raise ValueError(f"不支持的数据库类型：{source_config.type} / {target_config.type}")

    # Source and target discovery do not depend on each other. Running them in
    # parallel removes the sum of two metadata round-trip chains from latency.
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="flowdb-deep-side") as executor:
        source_future = executor.submit(_collect_side, source_config, "source", owners)
        target_future = executor.submit(_collect_side, target_config, "target")
        source_side = source_future.result()
        target_side = target_future.result()

    source_env = source_side["env"]
    target_env = target_side["env"]
    comparison = _build_comparison(source_env, target_env)
    risks = _build_risks(
        source_env, target_env,
        source_side["objects"], target_side["objects"],
        source_side["data"], target_side["data"],
        source_side["foreign_keys"], target_side["foreign_keys"],
        comparison,
    )

    # P1: per-column type mapping preview for source TOP tables (deep report)
    mapping_notes: list[dict] = []
    quality_notes: list[dict] = []
    p2_notes: list[dict] = []
    # These three probes are independent and each manages its own engine and
    # notes. Parallel execution keeps the report complete without serial waits.
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="flowdb-deep-probe") as executor:
        mapping_future = executor.submit(
            _augment_top_table_mappings,
            source_side["data"], source_config, target_config.type, mapping_notes,
        )
        quality_future = executor.submit(
            _collect_data_quality, source_config, target_config, quality_notes,
        )
        security_future = executor.submit(_collect_security, source_config, p2_notes)
        mapping_future.result()
        data_quality = quality_future.result()
        security = security_future.result()
    p1_notes = mapping_notes + quality_notes
    for table_quality in data_quality.get("tables", []):
        for table_risk in table_quality.get("risks", []):
            risks.append({**table_risk, "category": f"数据质量·{table_quality['table']}"})

    for side_label, side in (("源端", source_side), ("目标端", target_side)):
        connect_error = side.get("connect_error")
        if connect_error:
            risks.append({
                "level": "blocking",
                "category": "连接",
                "message": f"{side_label}连接失败：{str(connect_error)[:200]}，无法完成深度评估，请检查网络、端口、账号权限后重试",
            })

    # P2: performance pressure may append a warning risk before stats are computed
    performance = _performance_pressure(source_env, source_side["data"], source_config)
    if performance["level"] == "高":
        risks.append({
            "level": "warning",
            "category": "性能压力",
            "message": (
                f"迁移期间源端负载等级预估为高（最大表容量约 "
                f"{performance['max_table_bytes'] / 1024 ** 3:.1f} GB），"
                f"建议表并发上限 {performance['recommended_table_concurrency']}，并在业务低峰执行"
            ),
        })

    # 分区表分析（源端 Oracle）：间隔分区降级提示
    partition_analysis: dict[str, Any] = {
        "partitioned_total": 0, "interval_tables": [], "by_type": {}, "downgrades": [],
    }
    partition_notes: list[dict] = []
    if source_config.type == "oracle":
        partition_analysis = source_side.get("partition") or partition_analysis
        if partition_analysis.get("interval_tables"):
            names = partition_analysis["interval_tables"]
            shown = "、".join(str(n) for n in names[:10]) + ("…" if len(names) > 10 else "")
            risks.append({
                "level": "warning",
                "category": "分区",
                "message": (
                    f"检测到 {len(names)} 个 Oracle 间隔分区表（{shown}），"
                    "TDSQL 不支持间隔分区，迁移时将转换为普通 RANGE 分区表"
                ),
            })
            partition_notes.append({
                "section": "oracle.partition.interval",
                "message": f"间隔分区表 {len(names)} 个：{'、'.join(str(n) for n in names[:20])}，迁移时将转换为普通 RANGE 分区表",
            })

    time_estimate = _estimate_migration_time(source_config, source_side["data"], bandwidth_mbps, batch_size, table_concurrency)

    blocking = sum(1 for r in risks if r["level"] == "blocking")
    warnings = sum(1 for r in risks if r["level"] == "warning")
    score = max(0, min(100, 100 - blocking * 40 - warnings * 8))
    ready = blocking == 0
    suggestions = _build_suggestions(
        source_env, target_env, source_side["objects"], source_side["data"],
        source_side["foreign_keys"], risks,
    )
    conclusion = _build_conclusion(
        comparison, source_side["objects"], source_side["data"],
        source_side["foreign_keys"], data_quality, risks,
        score, ready, blocking,
    )

    all_notes = (
        [{"side": "source", **n} for n in source_side["env"].get("notes", [])]
        + [{"side": "target", **n} for n in target_side["env"].get("notes", [])]
        + [{"side": "source", **n} for n in p1_notes]
        + [{"side": "source", **n} for n in partition_notes]
        + [{"side": "source", **n} for n in p2_notes]
    )

    return {
        "generated_at": _now_iso(),
        "source": source_side,
        "target": target_side,
        "parameter_comparison": comparison,
        "object_stats": {
            "source": source_side["objects"],
            "target": target_side["objects"],
        },
        "data_analysis": {
            "source": source_side["data"],
            "target": target_side["data"],
        },
        "foreign_keys": {
            "source": source_side["foreign_keys"],
            "target": target_side["foreign_keys"],
        },
        "data_quality": data_quality,
        "partition_analysis": partition_analysis,
        "time_estimate": time_estimate,
        "conclusion": conclusion,
        "security": security,
        "performance": performance,
        "risks": risks,
        "summary": {
            "blocking": blocking,
            "warnings": warnings,
            "info": sum(1 for r in risks if r["level"] == "info"),
            "duration_ms": round((time.perf_counter() - started) * 1000),
        },
        "score": score,
        "ready": ready,
        "suggestions": suggestions,
        "notes": all_notes,
    }
