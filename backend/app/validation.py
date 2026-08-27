from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from itertools import zip_longest
from typing import Any

from sqlalchemy import Float, MetaData, Table, func, inspect, select, type_coerce
from sqlalchemy.sql.sqltypes import CHAR, NCHAR

from .database import default_schema, format_interval_ym, make_engine, selectable_columns
from .models import ConnectionConfig


def canonical(value: Any, target_type: Any | None = None) -> Any:
    if value is None:
        return ["null"]
    if hasattr(value, "read"):
        value = value.read()
    if isinstance(value, bytes):
        return ["bytes", len(value), base64.b64encode(hashlib.sha256(value).digest()).decode()]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, (int, float, Decimal)):
        # Compare source numbers at the precision actually stored by the
        # target column. Oracle FLOAT may arrive as Decimal while MySQL
        # DOUBLE is returned as float even though the migrated value matches.
        if isinstance(target_type, Float):
            value = float(value)
        decimal = Decimal(str(value))
        return ["number", format(decimal.normalize(), "f")]
    if isinstance(value, (datetime, date)):
        return ["datetime", value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()]
    if type(value).__name__ == "IntervalYM":
        # Oracle INTERVAL YEAR TO MONTH arrives as oracledb.IntervalYM.
        # Normalise to the Oracle canonical form (+0026-08) so it matches
        # the string value stored in the target column.
        return ["text", format_interval_ym(value)]
    text_value = str(value)
    # Oracle returns CHAR/NCHAR values padded to the declared width, while
    # MySQL/TDSQL normally removes that right padding when values are read.
    # Compare fixed-width character columns by their database semantics, but
    # keep leading spaces and keep VARCHAR/CLOB trailing spaces significant.
    if isinstance(target_type, (CHAR, NCHAR)):
        text_value = text_value.rstrip(" ")
    return ["text", text_value]


def database_object_names(engine, schema: str | None) -> list[str]:
    inspector = inspect(engine)
    return list(
        dict.fromkeys(
            inspector.get_table_names(schema=schema)
            + inspector.get_view_names(schema=schema)
        )
    )


def resolve_table_name(
    engine,
    schema: str | None,
    requested: str,
    names: list[str] | None = None,
) -> tuple[str, bool]:
    # Listing every table/view is expensive on Oracle. Validation resolves many
    # objects in one run, so callers can provide the list cached once per side.
    names = names if names is not None else database_object_names(engine, schema)
    if requested in names:
        return requested, True
    matches = [name for name in names if name.casefold() == requested.casefold()]
    if len(matches) == 1:
        return matches[0], False
    raise RuntimeError(f"找不到目标表：{requested}")


def hash_query(connection, statement, column_names: list[str], limit: int, target_types: dict[str, Any] | None = None) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    result = connection.execute(statement).mappings()
    while True:
        rows = result.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            if count >= limit:
                return count, digest.hexdigest()
            encoded = json.dumps(
                [canonical(row[name], target_types.get(name) if target_types else None) for name in column_names],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
            count += 1
    return count, digest.hexdigest()


def _encoded_row(row, column_names: list[str], target_types: dict[str, Any]) -> tuple[list[Any], bytes]:
    values = [canonical(row[name], target_types.get(name)) for name in column_names]
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    return values, encoded


def _row_stream(result, fetch_size: int):
    while True:
        rows = result.fetchmany(fetch_size)
        if not rows:
            return
        yield from rows


def compare_and_hash_queries(
    source_connection,
    target_connection,
    source_statement,
    target_statement,
    column_names: list[str],
    comparison_keys: list[str],
    target_types: dict[str, Any],
    sample_limit: int = 20,
    fetch_size: int = 5000,
) -> tuple[int, str, int, str, int, list[dict[str, Any]], dict[str, int]]:
    """Hash and compare both ordered streams in one pass per database."""
    source_result = source_connection.execute(source_statement).mappings()
    target_result = target_connection.execute(target_statement).mappings()
    source_digest = hashlib.sha256()
    target_digest = hashlib.sha256()
    source_hashed = 0
    target_hashed = 0
    difference_rows = 0
    samples: list[dict[str, Any]] = []
    column_counts: dict[str, int] = {}

    for row_index, (source_row, target_row) in enumerate(
        zip_longest(
            _row_stream(source_result, fetch_size),
            _row_stream(target_result, fetch_size),
        ),
        start=1,
    ):
        source_values = None
        target_values = None
        if source_row is not None:
            source_values, encoded = _encoded_row(source_row, column_names, target_types)
            source_digest.update(len(encoded).to_bytes(8, "big"))
            source_digest.update(encoded)
            source_hashed += 1
        if target_row is not None:
            target_values, encoded = _encoded_row(target_row, column_names, target_types)
            target_digest.update(len(encoded).to_bytes(8, "big"))
            target_digest.update(encoded)
            target_hashed += 1

        differences = []
        for index, name in enumerate(column_names):
            source_value = source_values[index] if source_values is not None else ["missing"]
            target_value = target_values[index] if target_values is not None else ["missing"]
            if source_value != target_value:
                column_counts[name] = column_counts.get(name, 0) + 1
                differences.append(
                    {"column": name, "source": source_value, "target": target_value}
                )
        if differences:
            difference_rows += 1
            if len(samples) < sample_limit:
                key_row = source_row if source_row is not None else target_row
                samples.append(
                    {
                        "row_index": row_index,
                        "primary_key": {
                            name: canonical(key_row[name], target_types.get(name))
                            for name in comparison_keys
                        }
                        if comparison_keys and key_row is not None
                        else {},
                        "columns": differences,
                    }
                )

    return (
        source_hashed,
        source_digest.hexdigest(),
        target_hashed,
        target_digest.hexdigest(),
        difference_rows,
        samples,
        column_counts,
    )


def compare_queries(source_connection, target_connection, source_statement, target_statement, column_names: list[str], primary_keys: list[str], target_types: dict[str, Any], sample_limit: int = 20) -> tuple[int, list[dict[str, Any]], dict[str, int]]:
    source_result = source_connection.execute(source_statement).mappings()
    target_result = target_connection.execute(target_statement).mappings()
    difference_rows = 0
    samples: list[dict[str, Any]] = []
    column_counts: dict[str, int] = {}
    row_index = 0
    while True:
        source_row = source_result.fetchone()
        target_row = target_result.fetchone()
        if source_row is None and target_row is None:
            break
        row_index += 1
        differences = []
        for name in column_names:
            source_value = canonical(source_row[name], target_types[name]) if source_row is not None else ["missing"]
            target_value = canonical(target_row[name], target_types[name]) if target_row is not None else ["missing"]
            if source_value != target_value:
                column_counts[name] = column_counts.get(name, 0) + 1
                differences.append({"column": name, "source": source_value, "target": target_value})
        if differences:
            difference_rows += 1
            if len(samples) < sample_limit:
                key_source = source_row if source_row is not None else target_row
                samples.append({
                    "row_index": row_index,
                    "primary_key": {name: canonical(key_source[name], target_types[name]) for name in primary_keys} if primary_keys and key_source is not None else {},
                    "columns": differences,
                })
    return difference_rows, samples, column_counts


def _comparison_key(
    inspector,
    table_name: str,
    schema: str | None,
    source_table: Table,
    configured_keys: list[str] | None = None,
) -> tuple[list[str], str]:
    primary_key = (
        inspector.get_pk_constraint(table_name, schema=schema).get(
            "constrained_columns"
        )
        or []
    )
    if primary_key:
        return primary_key, "primary_key"

    columns = {column.name.casefold(): column for column in source_table.columns}
    if configured_keys:
        keys = [columns[name.casefold()].name for name in configured_keys if name.casefold() in columns]
        if len(keys) == len(configured_keys) and all(not columns[name.casefold()].nullable for name in configured_keys):
            return keys, "business_key"

    for constraint in inspector.get_unique_constraints(table_name, schema=schema):
        names = constraint.get("column_names") or []
        if names and all(
            name.casefold() in columns and not columns[name.casefold()].nullable
            for name in names
        ):
            return [columns[name.casefold()].name for name in names], "unique_key"
    return [], "none"


def _validate_table(
    requested: str,
    payload: dict[str, Any],
    source_engine,
    target_engine,
    source_schema: str | None,
    target_schema: str | None,
    source_names: list[str],
    target_names: list[str],
    maximum: int,
    sample_limit: int,
    fetch_size: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    expected_target_name = payload.get("target_object_names", {}).get(
        requested, requested
    )
    source_name, _ = resolve_table_name(
        source_engine, source_schema, requested, source_names
    )
    target_name, expected_case_match = resolve_table_name(
        target_engine, target_schema, expected_target_name, target_names
    )
    source_table = Table(
        source_name, MetaData(), autoload_with=source_engine, schema=source_schema
    )
    target_table = Table(
        target_name, MetaData(), autoload_with=target_engine, schema=target_schema
    )
    column_names = [column.name for column in source_table.columns]
    target_columns = {
        column.name.casefold(): column.name for column in target_table.columns
    }
    if any(name.casefold() not in target_columns for name in column_names):
        raise RuntimeError(f"目标表字段不完整：{requested}")

    object_type = payload.get("object_types", {}).get(requested)
    if object_type == "view":
        comparison_keys, comparison_key_type = [], "none"
    else:
        comparison_keys, comparison_key_type = _comparison_key(
            inspect(source_engine),
            source_name,
            source_schema,
            source_table,
            payload.get("cdc_key_overrides", {}).get(requested),
        )
    source_statement = select(
        *selectable_columns(source_engine, target_engine, source_table)
    )
    target_types = {
        name: target_table.c[target_columns[name.casefold()]].type
        for name in column_names
    }
    target_expressions = []
    for name in column_names:
        column = target_table.c[target_columns[name.casefold()]]
        expression = (
            type_coerce(column, Float(asdecimal=False))
            if isinstance(column.type, Float)
            else column
        )
        target_expressions.append(expression.label(name))
    target_statement = select(*target_expressions)
    if comparison_keys:
        source_statement = source_statement.order_by(
            *[source_table.c[name] for name in comparison_keys]
        )
        target_statement = target_statement.order_by(
            *[
                target_table.c[target_columns[name.casefold()]]
                for name in comparison_keys
            ]
        )

    with (
        source_engine.connect().execution_options(stream_results=True) as source_connection,
        target_engine.connect().execution_options(stream_results=True) as target_connection,
    ):
        source_count = (
            source_connection.scalar(select(func.count()).select_from(source_table))
            or 0
        )
        target_count = (
            target_connection.scalar(select(func.count()).select_from(target_table))
            or 0
        )
        full_hash = (
            bool(comparison_keys)
            and source_count <= maximum
            and target_count <= maximum
        )
        source_hashed = target_hashed = 0
        source_hash = target_hash = None
        difference_rows = 0
        difference_samples: list[dict[str, Any]] = []
        column_difference_counts: dict[str, int] = {}
        if full_hash:
            (
                source_hashed,
                source_hash,
                target_hashed,
                target_hash,
                difference_rows,
                difference_samples,
                column_difference_counts,
            ) = compare_and_hash_queries(
                source_connection,
                target_connection,
                source_statement,
                target_statement,
                column_names,
                comparison_keys,
                target_types,
                sample_limit,
                fetch_size,
            )

    difference_types = []
    if source_count != target_count:
        difference_types.append(
            {
                "type": "row_count",
                "message": f"行数不一致：源端 {source_count} 行，目标端 {target_count} 行",
            }
        )
    if not expected_case_match:
        difference_types.append(
            {
                "type": "table_name",
                "message": f"目标对象名不符合任务策略：期望 {expected_target_name}，实际 {target_name}",
            }
        )
    if full_hash and source_hash != target_hash:
        difference_types.append(
            {
                "type": "row_values",
                "message": f"有 {difference_rows} 行存在字段值差异，已列出前 {len(difference_samples)} 行",
            }
        )
    passed = (
        source_count == target_count
        and expected_case_match
        and (not full_hash or source_hash == target_hash)
    )
    return {
        "table": requested,
        "target_table": target_name,
        "name_case_preserved": target_name == requested,
        "expected_target_table": expected_target_name,
        "source_rows": source_count,
        "target_rows": target_count,
        "row_count_equal": source_count == target_count,
        "hash_mode": "full" if full_hash else "count_only",
        "comparison_key_type": comparison_key_type,
        "comparison_keys": comparison_keys,
        "rows_hashed": min(source_hashed, target_hashed),
        "source_sha256": source_hash,
        "target_sha256": target_hash,
        "hash_equal": full_hash and source_hash == target_hash,
        "difference_types": difference_types,
        "difference_rows": difference_rows,
        "column_difference_counts": column_difference_counts,
        "difference_samples": difference_samples,
        "duration_ms": round((time.perf_counter() - started) * 1000),
        "passed": passed,
    }


def validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("migration_content") == "structure_only":
        raise RuntimeError("仅表结构任务没有迁移数据，不能执行行数和哈希校验")
    started = time.perf_counter()
    source_config = ConnectionConfig.model_validate(payload["source"])
    target_config = ConnectionConfig.model_validate(payload["target"])
    source_engine = make_engine(source_config)
    target_engine = make_engine(target_config)
    maximum = max(1, int(os.environ.get("FLOWDB_VALIDATION_MAX_ROWS", "100000")))
    sample_limit = max(
        0, int(os.environ.get("FLOWDB_VALIDATION_SAMPLE_ROWS", "20"))
    )
    fetch_size = max(
        100, int(os.environ.get("FLOWDB_VALIDATION_FETCH_SIZE", "5000"))
    )
    requested_workers = max(
        1, int(os.environ.get("FLOWDB_VALIDATION_CONCURRENCY", "4"))
    )
    tables = list(dict.fromkeys(payload["tables"]))
    workers = min(requested_workers, len(tables)) if tables else 1
    try:
        source_schema = default_schema(source_config)
        target_schema = default_schema(target_config)
        # These dictionary queries were previously repeated for every table.
        source_names = database_object_names(source_engine, source_schema)
        target_names = database_object_names(target_engine, target_schema)

        def validate_one(requested: str) -> dict[str, Any]:
            return _validate_table(
                requested,
                payload,
                source_engine,
                target_engine,
                source_schema,
                target_schema,
                source_names,
                target_names,
                maximum,
                sample_limit,
                fetch_size,
            )

        if workers == 1:
            results = [validate_one(table) for table in tables]
        else:
            with ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="flowdb-validation"
            ) as executor:
                # executor.map preserves the task's original table order.
                results = list(executor.map(validate_one, tables))
        return {
            "passed": all(item["passed"] for item in results),
            "tables": results,
            "max_hash_rows": maximum,
            "concurrency": workers,
            "duration_ms": round((time.perf_counter() - started) * 1000),
        }
    finally:
        source_engine.dispose()
        target_engine.dispose()
