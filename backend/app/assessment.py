from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from sqlalchemy import inspect, text

from .database import default_schema, make_engine, portable_type_info
from .models import ConnectionConfig


def _estimated_column_bytes(column: dict[str, Any]) -> int:
    source_type = column["type"]
    length = getattr(source_type, "length", None)
    name = source_type.__class__.__name__.upper()
    if length:
        return min(int(length), 4096)
    if any(token in name for token in ("LOB", "LONG", "TEXT", "XML", "JSON")):
        return 4096
    if any(token in name for token in ("DATE", "TIME")):
        return 32
    if any(token in name for token in ("NUMBER", "INT", "FLOAT", "DOUBLE", "DECIMAL")):
        return 16
    return 64


def _qualified_name(engine, schema: str | None, table: str) -> str:
    """Build a quoted name without invoking fragile full table reflection."""
    preparer = engine.dialect.identifier_preparer
    parts = []
    if schema:
        parts.append(preparer.quote_schema(schema))
    parts.append(preparer.quote_identifier(table))
    return ".".join(parts)


def _assess_table(
    requested: str,
    payload: dict[str, Any],
    source_engine,
    target_engine,
    source_schema: str | None,
    target_names: list[str],
) -> dict[str, Any]:
    # Inspector instances are not documented as thread-safe, so each worker owns one.
    source_inspector = inspect(source_engine)
    planned_target_name = payload.get("target_object_names", {}).get(requested, requested)
    object_type = payload.get("object_types", {}).get(requested, "table")
    columns = source_inspector.get_columns(requested, schema=source_schema)
    if not columns:
        raise RuntimeError(f"源表不存在或无字段：{requested}")
    primary_keys = [] if object_type == "view" else (
        source_inspector.get_pk_constraint(requested, schema=source_schema).get("constrained_columns") or []
    )
    with source_engine.connect() as connection:
        row_count = int(connection.scalar(text(
            f"SELECT COUNT(*) FROM {_qualified_name(source_engine, source_schema, requested)}"
        )) or 0)
    table_estimated_bytes = row_count * sum(_estimated_column_bytes(column) for column in columns)
    exact_target = planned_target_name in target_names
    case_matches = [name for name in target_names if name.casefold() == planned_target_name.casefold()]
    target_name = planned_target_name if exact_target else case_matches[0] if len(case_matches) == 1 else None
    risks: list[dict[str, str]] = []
    if object_type == "view":
        risks.append({"level": "warning", "code": "VIEW_MATERIALIZED", "message": "跨数据库视图将迁移为目标端实体表，保存评估时刻的查询结果；原视图 SQL 不会直接转换"})
    if planned_target_name != requested:
        risks.append({"level": "warning", "code": "NAME_TRANSFORM", "message": f"按目标命名策略，源对象 {requested} 将创建为 {planned_target_name}"})
    mappings = []
    for column in columns:
        source_type = column["type"]
        source_type_name = str(source_type)
        source_class = source_type.__class__.__name__.upper()
        identity = bool(column.get("identity")) or column.get("autoincrement") is True
        mapped_info = portable_type_info(source_type, target_engine.dialect.name, identity)
        mapped = mapped_info["target_type"]
        mappings.append({
            "column": column["name"],
            "source_type": source_type_name,
            "target_type": str(mapped.compile(dialect=target_engine.dialect)),
            "nullable": bool(column.get("nullable", True)),
            "identity": identity,
            "degraded": mapped_info["degraded"],
            "degradation": mapped_info["degradation"],
        })
        if "BFILE" in source_class:
            risks.append({"level": "blocking", "code": "BFILE_EXTERNAL", "message": f"字段 {column['name']} 是 BFILE，必须保证 Oracle 外部目录和文件在迁移期间可读取"})
        elif source_class in {"LONG", "LONG_RAW"}:
            risks.append({"level": "warning", "code": "LEGACY_LOB", "message": f"字段 {column['name']} 使用 {source_class}，建议迁移后重点校验长度与哈希"})
        if "TIMESTAMP" in source_class and bool(getattr(source_type, "timezone", False)) and payload["target"]["type"] == "mysql":
            risks.append({"level": "warning", "code": "TIMEZONE_TO_TEXT", "message": f"字段 {column['name']} 含时区，迁移到 MySQL 时保存为带偏移量文本"})
    if not primary_keys:
        risks.append({"level": "warning", "code": "NO_PRIMARY_KEY", "message": "无主键：无法执行稳定排序的逐行全字段哈希，将降级为行数校验"})
        if target_engine.dialect.name == "mysql":
            risks.append({"level": "warning", "code": "SYNTHETIC_PRIMARY_KEY", "message": "目标 MySQL/TDSQL 将增加内部自增主键 __flowdb_row_id，以兼容强制主键策略"})
    if target_name:
        strategy = payload["existing_table"]
        level = "blocking" if strategy == "fail" else "warning"
        risks.append({"level": level, "code": "TARGET_EXISTS", "message": f"目标表已存在，将按“{strategy}”策略处理"})
        if target_name != requested:
            risks.append({"level": "warning", "code": "NAME_CASE", "message": f"目标表实际名称为 {target_name}，大小写与源表不同"})
    table_blocking = sum(1 for item in risks if item["level"] == "blocking")
    table_warnings = sum(1 for item in risks if item["level"] == "warning")
    return {
        "table": requested,
        "object_type": object_type,
        "rows": row_count,
        "columns": len(columns),
        "primary_keys": primary_keys,
        "estimated_bytes": table_estimated_bytes,
        "target_exists": bool(target_name),
        "target_name": target_name,
        "planned_target_name": planned_target_name,
        "blocking_count": table_blocking,
        "warning_count": table_warnings,
        "risks": risks,
        "column_mappings": mappings,
    }


def assess_payload(payload: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    source = ConnectionConfig.model_validate(payload["source"])
    target = ConnectionConfig.model_validate(payload["target"])
    source_engine = make_engine(source)
    target_engine = make_engine(target)
    source_schema = default_schema(source)
    target_schema = default_schema(target)
    requested_tables = list(payload["tables"])
    try:
        target_names = inspect(target_engine).get_table_names(schema=target_schema)
        configured_workers = int(os.getenv("FLOWDB_ASSESSMENT_CONCURRENCY", "4"))
        workers = max(1, min(configured_workers, len(requested_tables) or 1))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="flowdb-assess") as executor:
            report_tables = list(executor.map(
                lambda requested: _assess_table(
                    requested, payload, source_engine, target_engine, source_schema, target_names
                ),
                requested_tables,
            ))
        total_rows = sum(table["rows"] for table in report_tables)
        estimated_bytes = sum(table["estimated_bytes"] for table in report_tables)
        blocking = sum(table["blocking_count"] for table in report_tables)
        warnings = sum(table["warning_count"] for table in report_tables)
        score = max(0, 100 - blocking * 25 - warnings * 4)
        return {
            "ready": blocking == 0,
            "score": score,
            "summary": {
                "tables": len(report_tables),
                "rows": total_rows,
                "estimated_bytes": estimated_bytes,
                "blocking": blocking,
                "warnings": warnings,
                "batch_size": payload["batch_size"],
                "table_concurrency": min(payload.get("table_concurrency", 1), len(report_tables)),
                "assessment_concurrency": workers,
                "duration_ms": round((time.perf_counter() - started) * 1000),
            },
            "tables": report_tables,
        }
    finally:
        source_engine.dispose()
        target_engine.dispose()
