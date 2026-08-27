from __future__ import annotations

import logging
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any

from .cdc import IncrementalReplicator, make_logminer_engine
from .database import copy_batches, default_schema, make_engine, prepare_table
from .models import ConnectionConfig
from .store import JobStore, utc_now
from sqlalchemy import text

logger = logging.getLogger("flowdb.worker")


def migration_error_message(exc: Exception, object_name: str) -> str:
    """Return an actionable error without discarding the driver/SQL detail."""
    raw = str(exc).strip() or repr(exc)
    error_type = exc.__class__.__name__
    lowered = raw.lower()
    if "disk is full" in lowered or "(3675" in raw:
        headline = "目标端空间或实例配额不足（TDSQL 3675），无法创建对象"
    elif "lock wait timeout" in lowered or "(1205" in raw:
        headline = "目标端元数据锁等待超时（TDSQL/MySQL 1205）"
    elif "源端对象反射失败" in raw or error_type in {"NoSuchTableError", "KeyError"}:
        headline = "源端对象元数据反射失败，请重新读取对象并核对 Schema 与名称大小写"
    else:
        headline = "数据库迁移执行失败"
    return f"{headline}\n对象：{object_name}\n异常类型：{error_type}\n原始错误：{raw}"


def ordered_migration_phases(
    objects: list[str],
    object_types: dict[str, str],
    sequences: list[str] | None = None,
) -> tuple[tuple[str, list[str]], ...]:
    """Keep dependency-bearing views out of the queue until every table is done.

    序列（sequence）必须先于表创建，因为表 DEFAULT 子句可能引用 sequence.nextval。
    普通表必须先于分区表（普通表在上、分区表在下），视图最后。
    """
    phases: list[tuple[str, list[str]]] = []
    if sequences:
        phases.append(("序列迁移", sequences))
    phases.append(("普通表迁移", [name for name in objects if object_types.get(name, "table") in (None, "table")]))
    phases.append(("分区表迁移", [name for name in objects if object_types.get(name, "table") == "partitioned_table"]))
    phases.append(("视图迁移", [name for name in objects if object_types.get(name, "table") == "view"]))
    return tuple(phases)


def _build_tdsql_sequence_ddl(seq_name: str, meta: dict[str, Any]) -> str:
    """根据 Oracle 序列元数据生成 TDSQL 序列 DDL。

    TDSQL 支持：CREATE TDSQL_SEQUENCE name START WITH n INCREMENT BY n
    MINVALUE n MAXVALUE n CYCLE|NOCYCLE CACHE n
    """
    # TDSQL 序列底层为 BIGINT，Oracle 默认 MAXVALUE 可达 1e27，超出后
    # TDSQL 解析器直接报 1064，必须收敛到 BIGINT 合法范围。
    _TDSQL_MIN = -9223372036854775808
    _TDSQL_MAX = 9223372036854775807
    start = min(_TDSQL_MAX, max(_TDSQL_MIN, int(meta.get("last_number") or 1)))
    increment = int(meta.get("increment_by") or 1)
    min_value = min(_TDSQL_MAX, max(_TDSQL_MIN, int(meta.get("min_value") or 1)))
    max_value = min(_TDSQL_MAX, max(_TDSQL_MIN, int(meta.get("max_value") or _TDSQL_MAX)))
    if max_value < min_value:
        max_value = min_value
    cache = int(meta.get("cache_size") or 20)
    cycle = "CYCLE" if str(meta.get("cycle_flag") or "N").upper() == "Y" else "NOCYCLE"
    quoted_name = f"`{seq_name.replace('`', '``')}`"
    return (
        f"CREATE TDSQL_SEQUENCE {quoted_name} START WITH {start} "
        f"INCREMENT BY {increment} MINVALUE {min_value} MAXVALUE {max_value} "
        f"{cycle} CACHE {cache}"
    )


def _fetch_oracle_sequence_meta(engine, owner: str | None, seq_name: str) -> dict[str, Any] | None:
    """从源端 Oracle 读取单个序列的元数据（按 schema + 名称精确匹配）。"""
    if not owner:
        return None
    sql = text(
        "SELECT sequence_name, min_value, max_value, increment_by, last_number, cache_size, cycle_flag "
        "FROM all_sequences WHERE sequence_owner = :owner AND sequence_name = :name"
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"owner": owner, "name": seq_name}).mappings().first()
    return dict(row) if row else None


def _target_supports_tdsql_sequences(engine, config: ConnectionConfig) -> tuple[bool, str]:
    """Detect TDSQL sequence support even when the saved link uses MySQL type.

    Older links identify TDSQL through its MySQL-compatible driver, so checking
    only ``config.type == "tdsql"`` silently discarded selected sequences.
    """
    if config.type == "tdsql":
        return True, "链路类型为 TDSQL"
    if config.type != "mysql":
        return False, f"目标类型 {config.type} 不支持 TDSQL_SEQUENCE"
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT VERSION() AS version, @@version_comment AS version_comment")
            ).mappings().first()
    except Exception as exc:
        return False, f"目标端序列能力探测失败：{exc}"
    signature = " ".join(str(value or "") for value in (row or {}).values()).lower()
    if "tdsql" in signature or "txsql" in signature:
        return True, f"检测到 TDSQL/TXSQL：{signature[:180]}"
    return False, f"目标端不是支持 TDSQL_SEQUENCE 的 TDSQL/TXSQL：{signature[:180] or '版本信息为空'}"


def _execute_tdsql_sequence_ddl(
    engine,
    statements: list[str],
    timeout_seconds: float = 8.0,
) -> None:
    """Run sequence DDL with a hard client-side deadline.

    Some TDSQL proxy versions ignore ``lock_wait_timeout`` for sequence metadata
    locks.  A daemon worker plus a second control connection lets the migration
    phase continue even when the proxy leaves the DDL session in ``starting``.
    """
    finished = threading.Event()
    connection_ready = threading.Event()
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            with engine.connect() as conn:
                connection_id = int(conn.execute(text("SELECT CONNECTION_ID()")).scalar_one())
                outcome["connection_id"] = connection_id
                connection_ready.set()
                conn.execute(text("SET SESSION lock_wait_timeout = 5"))
                for statement in statements:
                    conn.execute(text(statement))
                conn.commit()
        except BaseException as exc:  # propagated on the migration thread
            outcome["error"] = exc
        finally:
            connection_ready.set()
            finished.set()

    ddl_thread = threading.Thread(
        target=execute,
        name="flowdb-sequence-ddl",
        daemon=True,
    )
    ddl_thread.start()
    if finished.wait(timeout_seconds):
        if "error" in outcome:
            raise outcome["error"]
        return

    connection_ready.wait(0.2)
    connection_id = outcome.get("connection_id")
    if connection_id is not None:
        try:
            with engine.begin() as control:
                control.execute(text(f"KILL {int(connection_id)}"))
        except Exception:
            logger.warning("Unable to kill timed-out TDSQL sequence session %s", connection_id)
    raise TimeoutError(
        f"TDSQL 序列 DDL 超过 {timeout_seconds:g} 秒，已终止该数据库会话并继续后续对象"
    )


class MigrationRunner:
    def __init__(self, store: JobStore, workers: int = 2):
        self.store = store
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="flowdb-job")
        self.active: set[str] = set()
        self.stop_events: dict[str, threading.Event] = {}
        self.lock = threading.Lock()

    def submit(self, job_id: str) -> None:
        with self.lock:
            if job_id in self.active:
                return
            self.active.add(job_id)
            self.stop_events[job_id] = threading.Event()
        self.pool.submit(self._run, job_id)

    def request_cancel(self, job_id: str) -> bool:
        """Notify the in-process worker immediately as well as the durable flag."""
        with self.lock:
            stop_event = self.stop_events.get(job_id)
            if stop_event is None:
                return False
            stop_event.set()
            return True

    def submit_when_idle(self, job_id: str) -> None:
        """Restart a stopped CDC job after its previous worker has exited.

        ``finish-sync`` returns immediately after signalling the LogMiner loop.
        A user can therefore click “继续增量同步” before the old worker reaches
        its ``finally`` block.  Queue the restart behind that cleanup instead of
        silently dropping it because the job id is still marked active.
        """
        def wait_and_submit() -> None:
            while True:
                with self.lock:
                    idle = job_id not in self.active
                if idle:
                    self.submit(job_id)
                    return
                time.sleep(0.05)

        with self.lock:
            idle = job_id not in self.active
        if idle:
            self.submit(job_id)
        else:
            threading.Thread(
                target=wait_and_submit,
                name=f"flowdb-resume-{job_id[:8]}",
                daemon=True,
            ).start()

    def _run(self, job_id: str) -> None:
        source_engine = target_engine = cdc_engine = None
        with self.lock:
            stop_event = self.stop_events.setdefault(job_id, threading.Event())
        try:
            if stop_event.is_set() or self.store.cancelled(job_id):
                self.store.update(
                    job_id,
                    status="cancelled",
                    finished_at=utc_now(),
                    current_table=None,
                )
                return
            previous_state = self.store.get(job_id)
            payload = self.store.get_payload(job_id)
            fail_policy = payload.get("fail_policy", "stop_on_error")
            sync_mode = payload.get("sync_mode", "full_only")
            cdc_runtime_mode = sync_mode in {"full_and_incremental", "incremental_only"}
            capture_for_later = sync_mode == "full_then_incremental"
            resume_incremental = (
                cdc_runtime_mode
                and previous_state.get("sync_phase") in {"catching_up", "realtime", "incremental"}
                and previous_state.get("checkpoint_scn")
            )
            self.store.update(
                job_id,
                status="running",
                started_at=utc_now(),
                current_table="正在连接源端和目标端",
                error=None,
                table_results=[],
            )
            self.store.append_log(job_id, "INFO", "任务开始，正在连接源端和目标端")
            source = ConnectionConfig.model_validate(payload["source"])
            target = ConnectionConfig.model_validate(payload["target"])
            source_engine = make_engine(source)
            target_engine = make_engine(target)
            self.store.append_log(job_id, "INFO", "源端与目标端连接成功")
            replicator = None
            start_scn = None
            if cdc_runtime_mode or capture_for_later:
                if source.type != "oracle" or target.type not in {"mysql", "tdsql"}:
                    raise RuntimeError("全量+增量当前仅支持 Oracle → MySQL/TDSQL")
                cdc_engine = make_logminer_engine(source)
                replicator = IncrementalReplicator(
                    self.store,
                    job_id,
                    payload,
                    source_engine,
                    target_engine,
                    stop_event,
                    logminer_engine=cdc_engine,
                )
                start_scn = int(
                    previous_state.get("checkpoint_scn")
                    or payload.get("start_scn")
                    or replicator.miner.capture_start_scn()
                )
                capabilities = replicator.preflight(start_scn=start_scn)
                self.store.update(
                    job_id,
                    start_scn=int(previous_state.get("start_scn") or start_scn),
                    checkpoint_scn=start_scn,
                    sync_phase="incremental" if sync_mode == "incremental_only" else "full",
                    progress=100 if sync_mode == "incremental_only" else previous_state.get("progress", 0),
                    tables_completed=(
                        previous_state.get("tables_total", 0)
                        if sync_mode == "incremental_only"
                        else previous_state.get("tables_completed", 0)
                    ),
                )
                self.store.append_log(
                    job_id,
                    "INFO",
                    "LogMiner 前置检查通过："
                    f"ARCHIVELOG={capabilities['log_mode']}，"
                    f"最小补充日志={capabilities['supplemental_log_data_min']}，起始 SCN={start_scn}",
                )
                if sync_mode == "incremental_only" or resume_incremental:
                    if resume_incremental:
                        self.store.append_log(job_id, "INFO", f"服务恢复，从检查点 SCN {start_scn} 继续增量同步")
                    replicator.run(start_scn)
                    if stop_event.is_set() or self.store.cancelled(job_id):
                        # A normal finish marks the job completed before it
                        # signals this loop.  Preserve that terminal state;
                        # only an explicit cancellation becomes cancelled.
                        if self.store.get(job_id)["status"] != "completed":
                            self.store.update(
                                job_id,
                                status="cancelled",
                                sync_phase="stopped",
                                current_table=None,
                                finished_at=utc_now(),
                            )
                    return
            rows_total = 0
            bytes_total = 0
            tables = payload["tables"]
            target_object_names = payload.get("target_object_names", {})
            requested_case_policy = payload.get("identifier_case_policy", "auto")
            resolved_case_policy = payload.get("identifier_case_resolved", "preserve")
            lower_case_table_names = payload.get("target_lower_case_table_names")
            detected_text = (
                "不适用" if lower_case_table_names is None else str(lower_case_table_names)
            )
            self.store.append_log(
                job_id,
                "INFO",
                f"目标表名策略：请求={requested_case_policy}，lower_case_table_names={detected_text}，实际={resolved_case_policy}",
            )
            mapped_pairs = [
                f"{source_name} → {target_object_names.get(source_name, source_name)}"
                for source_name in tables
                if target_object_names.get(source_name, source_name) != source_name
            ]
            if mapped_pairs:
                self.store.append_log(job_id, "INFO", "目标对象名称映射：" + "；".join(mapped_pairs[:50]))
            migration_content = payload.get("migration_content", "structure_and_data")
            sequences = payload.get("sequences", [])
            migrate_sequences = payload.get("migrate_sequences", True)
            # 用户名映射：源端 owner/schema → 目标端用户名（大小写不敏感匹配）
            user_mappings_raw = payload.get("user_mappings", []) or []
            user_mappings = {}
            for mapping in user_mappings_raw:
                source_user = str(mapping.get("source", "")).strip()
                target_user = str(mapping.get("target", "")).strip()
                if source_user and target_user:
                    user_mappings[source_user.upper()] = target_user
            default_target_schema = default_schema(target)
            completed = 0
            table_results: list[dict[str, Any]] = []
            object_types = payload.get("object_types", {})
            effective_sequences: list[str] = []
            if migrate_sequences and sequences:
                supports_sequences, support_detail = _target_supports_tdsql_sequences(
                    target_engine, target
                )
                if not supports_sequences:
                    message = (
                        f"已选择 {len(sequences)} 个序列，但无法执行序列迁移："
                        f"{support_detail}"
                    )
                    self.store.append_log(job_id, "ERROR", message)
                    raise RuntimeError(message)
                effective_sequences = sequences
                self.store.append_log(
                    job_id,
                    "INFO",
                    f"序列迁移能力检测通过，将先迁移 {len(sequences)} 个序列（{support_detail}）",
                )
            total_objects = len(tables) + len(effective_sequences)
            phases = ordered_migration_phases(tables, object_types, effective_sequences)
            phase_keys = {
                "序列迁移": "sequence",
                "普通表迁移": "table",
                "分区表迁移": "partitioned_table",
                "视图迁移": "view",
            }

            def mapped_target_schema(table_name: str, source_schema: str | None) -> str | None:
                """按表归属的源 owner 查找用户名映射，命中则目标 schema 使用映射后的用户名。"""
                owner = None
                if "." in table_name:
                    owner = table_name.split(".", 1)[0].strip()
                if not owner and source_schema:
                    owner = source_schema
                if owner and owner.upper() in user_mappings:
                    return user_mappings[owner.upper()]
                return default_target_schema

            state_lock = threading.Lock()
            active_tables: set[str] = set()
            phase_label = "准备迁移"
            sequence_lock_blocked = threading.Event()

            def phase_progress_snapshot() -> list[dict[str, Any]]:
                snapshot: list[dict[str, Any]] = []
                for label, phase_objects in phases:
                    phase_key = phase_keys[label]
                    names = set(phase_objects)
                    results = [
                        item
                        for item in table_results
                        if item.get("table") in names
                        and (item.get("object_type") or "table") == phase_key
                    ]
                    succeeded = sum(item.get("status") == "success" for item in results)
                    failed = sum(item.get("status") == "failed" for item in results)
                    cancelled = sum(item.get("status") in {"cancelled", "skipped"} for item in results)
                    finished = succeeded + failed + cancelled
                    running_objects = sorted(active_tables.intersection(names))
                    total = len(phase_objects)
                    snapshot.append(
                        {
                            "phase": phase_key,
                            "label": label,
                            "total": total,
                            "completed": succeeded,
                            "failed": failed,
                            "cancelled": cancelled,
                            "running": len(running_objects),
                            "pending": max(total - finished - len(running_objects), 0),
                            "progress": round(finished / max(total, 1) * 100, 2) if total else 100,
                            "current_objects": running_objects,
                        }
                    )
                return snapshot

            def update_active() -> None:
                # 取消接口会立即清空 current_table。工作线程与接口线程并发时，
                # 不允许迟到的进度刷新把已取消任务的“当前表”重新写回来。
                if stop_event.is_set() or self.store.cancelled(job_id):
                    return
                current = ", ".join(sorted(active_tables))
                self.store.update(
                    job_id,
                    current_table=(f"{phase_label} · {current}" if current else phase_label)[:500],
                    phase_progress=phase_progress_snapshot(),
                )
                # 覆盖“检查后、写入前”恰好发生取消的极小竞态窗口。
                if stop_event.is_set() or self.store.cancelled(job_id):
                    self.store.update(job_id, current_table=None)

            def migrate_table(table_name: str) -> dict[str, Any]:
                nonlocal rows_total, bytes_total
                result: dict[str, Any] = {
                    "table": table_name,
                    "target_table": target_object_names.get(table_name, table_name),
                    "object_type": object_types.get(table_name, "table") or "table",
                    "status": "running",
                    "rows": 0,
                    "bytes": 0,
                    "error": None,
                    "started_at": utc_now(),
                    "finished_at": None,
                    "elapsed_ms": 0,
                }
                started = time.perf_counter()
                if stop_event.is_set() or self.store.cancelled(job_id):
                    result["status"] = "skipped"
                    return result
                with state_lock:
                    active_tables.add(table_name)
                    update_active()
                try:
                    self.store.append_log(job_id, "INFO", f"[{table_name}] 开始处理，准备目标表")
                    prepared = prepare_table(
                        source_engine, target_engine, default_schema(source),
                        mapped_target_schema(table_name, default_schema(source)),
                        table_name,
                        payload["existing_table"], migration_content != "data_only",
                        target_object_names.get(table_name, table_name),
                    )
                    if stop_event.is_set() or self.store.cancelled(job_id):
                        result["status"] = "cancelled"
                        self.store.append_log(job_id, "WARN", f"[{table_name}] 已取消，停止迁移")
                        return result
                    if prepared.partition_warnings:
                        for warn_text in prepared.partition_warnings:
                            self.store.append_log(job_id, "WARN", f"[{table_name}] {warn_text}")
                        result["notes"] = "; ".join(prepared.partition_warnings)
                    if prepared.partition_info and prepared.partition_info.get("interval") == "YES":
                        interval_warn = f"表 {table_name} 为 Oracle 间隔分区表，已转换为普通 RANGE 分区表（TDSQL 不支持间隔分区）"
                        self.store.append_log(job_id, "WARN", f"[{table_name}] {interval_warn}")
                        result["notes"] = (result.get("notes") + "; " if result.get("notes") else "") + interval_warn
                    if migration_content == "structure_only":
                        self.store.append_log(job_id, "INFO", f"[{table_name}] 结构创建完成")
                    else:
                        self.store.append_log(job_id, "INFO", f"[{table_name}] 目标表就绪，开始写入数据")
                        for row_count, byte_count in copy_batches(source_engine, target_engine, prepared, payload["batch_size"]):
                            if stop_event.is_set() or self.store.cancelled(job_id):
                                result["status"] = "cancelled"
                                self.store.append_log(job_id, "WARN", f"[{table_name}] 收到取消请求，停止写入")
                                return result
                            with state_lock:
                                rows_total += row_count
                                bytes_total += byte_count
                                result["rows"] += row_count
                                result["bytes"] += byte_count
                                self.store.update(job_id, rows_copied=rows_total, bytes_copied=bytes_total)
                            self.store.append_log(job_id, "INFO", f"[{table_name}] 已写入 {result['rows']} 行")
                    result["status"] = "success"
                    result["finished_at"] = utc_now()
                    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
                    self.store.append_log(job_id, "INFO", f"[{table_name}] 迁移完成，共 {result['rows']} 行，耗时 {result['elapsed_ms']} ms")
                    return result
                except Exception as exc:
                    result["status"] = "failed"
                    result["error"] = migration_error_message(exc, table_name)
                    result["finished_at"] = utc_now()
                    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
                    self.store.append_log(job_id, "ERROR", f"[{table_name}] 迁移失败：{result['error']}")
                    return result
                finally:
                    with state_lock:
                        active_tables.discard(table_name)
                        update_active()

            def migrate_sequence(seq_name: str) -> dict[str, Any]:
                result: dict[str, Any] = {
                    "table": seq_name,
                    "object_type": "sequence",
                    "status": "running",
                    "rows": 0,
                    "bytes": 0,
                    "error": None,
                    "started_at": utc_now(),
                    "finished_at": None,
                    "elapsed_ms": 0,
                }
                started = time.perf_counter()
                if sequence_lock_blocked.is_set():
                    result["status"] = "failed"
                    result["error"] = "目标端序列元数据锁异常，本任务跳过剩余序列并继续迁移表"
                    result["finished_at"] = utc_now()
                    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
                    self.store.append_log(job_id, "WARN", f"[{seq_name}] {result['error']}")
                    return result
                if stop_event.is_set() or self.store.cancelled(job_id):
                    result["status"] = "skipped"
                    return result
                with state_lock:
                    active_tables.add(seq_name)
                    update_active()
                try:
                    self.store.append_log(job_id, "INFO", f"[{seq_name}] 开始迁移序列")
                    seq_parts = seq_name.split(".")
                    seq_name_clean = seq_parts[-1]
                    seq_owner = seq_parts[0] if len(seq_parts) > 1 else None
                    if seq_owner:
                        meta = _fetch_oracle_sequence_meta(source_engine, seq_owner, seq_name_clean)
                    else:
                        meta = _fetch_oracle_sequence_meta(source_engine, default_schema(source), seq_name_clean)
                        if not meta:
                            # 多 owner 场景：按名称在所有可访问 schema 中查找
                            with source_engine.connect() as conn:
                                rows = conn.execute(
                                    text("SELECT sequence_owner FROM all_sequences WHERE sequence_name = :name"),
                                    {"name": seq_name_clean},
                                ).mappings()
                                for row in rows:
                                    meta = _fetch_oracle_sequence_meta(
                                        source_engine, str(row["sequence_owner"]), seq_name_clean
                                    )
                                    if meta:
                                        break
                    if not meta:
                        raise RuntimeError(f"源端未找到序列 {seq_name} 的元数据")
                    if stop_event.is_set() or self.store.cancelled(job_id):
                        result["status"] = "cancelled"
                        self.store.append_log(job_id, "WARN", f"[{seq_name}] 已取消，停止迁移序列")
                        return result
                    ddl = _build_tdsql_sequence_ddl(seq_name_clean, meta)
                    quoted_name = f"`{seq_name_clean.replace('`', '``')}`"
                    statements: list[str] = []
                    # TDSQL 的序列锁不一定遵守 lock_wait_timeout，因此 DROP 与
                    # CREATE 必须放进带硬超时和 KILL 保护的专用连接。
                    if payload.get("existing_table") == "drop_and_create":
                        statements.append(f"DROP TDSQL_SEQUENCE IF EXISTS {quoted_name}")
                    statements.append(ddl)
                    try:
                        _execute_tdsql_sequence_ddl(target_engine, statements)
                    except Exception as exc:
                        if "already exists" in str(exc).lower() or "4049" in str(exc):
                            self.store.append_log(job_id, "WARN", f"[{seq_name}] 序列已存在，跳过创建（幂等重跑）")
                            result["status"] = "success"
                            result["finished_at"] = utc_now()
                            result["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
                            return result
                        if isinstance(exc, TimeoutError) or "1205" in str(exc):
                            sequence_lock_blocked.set()
                        raise
                    if stop_event.is_set() or self.store.cancelled(job_id):
                        result["status"] = "cancelled"
                        self.store.append_log(job_id, "WARN", f"[{seq_name}] 已取消，停止迁移序列")
                        return result
                    result["status"] = "success"
                    result["finished_at"] = utc_now()
                    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
                    self.store.append_log(job_id, "INFO", f"[{seq_name}] 序列创建完成：{ddl}")
                    return result
                except Exception as exc:
                    result["status"] = "failed"
                    result["error"] = migration_error_message(exc, seq_name)
                    result["finished_at"] = utc_now()
                    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
                    self.store.append_log(job_id, "ERROR", f"[{seq_name}] 序列迁移失败：{result['error']}")
                    return result
                finally:
                    with state_lock:
                        active_tables.discard(seq_name)
                        update_active()

            self.store.update(job_id, phase_progress=phase_progress_snapshot())
            self.store.append_log(
                job_id,
                "INFO",
                "执行计划："
                + " → ".join(f"{label} {len(items)} 个" for label, items in phases),
            )
            overall_failed = False
            for current_phase, phase_objects in phases:
                if not phase_objects:
                    continue
                phase_label = current_phase
                # TDSQL_SEQUENCE 并发 DDL 会竞争元数据锁。序列本身很快，
                # 强制串行可避免两个 CREATE/DROP 互相等待。
                concurrency = 1 if current_phase == "序列迁移" else min(
                    int(payload.get("table_concurrency", 1)), len(phase_objects)
                )
                self.store.append_log(job_id, "INFO", f"阶段开始：{current_phase}，共 {len(phase_objects)} 个对象，并发 {concurrency}")
                if current_phase == "序列迁移" and len(phase_objects) > 1:
                    self.store.append_log(job_id, "INFO", "序列阶段采用串行 DDL，避免 TDSQL 元数据锁冲突")
                with state_lock:
                    update_active()
                table_pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix=f"flowdb-table-{job_id[:6]}")
                futures = {
                    table_pool.submit(migrate_sequence if current_phase == "序列迁移" else migrate_table, table_name): table_name
                    for table_name in phase_objects
                }
                try:
                    pending_futures = set(futures)
                    while pending_futures:
                        if stop_event.is_set() or self.store.cancelled(job_id):
                            stop_event.set()
                            for queued in pending_futures:
                                queued.cancel()
                            self.store.update(job_id, status="cancelled", finished_at=utc_now(), current_table=None, table_results=table_results, phase_progress=phase_progress_snapshot())
                            return
                        done, pending_futures = wait(
                            pending_futures,
                            timeout=0.25,
                            return_when=FIRST_COMPLETED,
                        )
                        for future in done:
                            result = future.result()
                            if not result:
                                continue
                            if result not in table_results:
                                table_results.append(result)
                                self.store.update(job_id, table_results=table_results, phase_progress=phase_progress_snapshot())
                            if result["status"] == "failed":
                                overall_failed = True
                                if fail_policy == "stop_on_error":
                                    self.store.append_log(job_id, "ERROR", f"{result['table']} 失败，失败策略为停止，终止任务")
                                    stop_event.set()
                                    for queued in pending_futures:
                                        queued.cancel()
                                    self.store.update(
                                        job_id,
                                        status="failed",
                                        error=f"对象 {result['table']} 迁移失败\n{result.get('error') or '未返回错误详情'}",
                                        finished_at=utc_now(),
                                        current_table=None,
                                        table_results=table_results,
                                        phase_progress=phase_progress_snapshot(),
                                    )
                                    return
                                self.store.append_log(job_id, "WARN", f"{result['table']} 失败，失败策略为继续，跳过该对象继续迁移")
                            elif result["status"] == "success":
                                completed += 1
                                self.store.update(job_id, tables_completed=completed, progress=round(completed / max(total_objects, 1) * 100, 2))
                except Exception:
                    stop_event.set()
                    for pending in futures:
                        pending.cancel()
                    raise
                finally:
                    table_pool.shutdown(wait=not stop_event.is_set(), cancel_futures=True)
                phase_results = [item for item in table_results if item.get("table") in set(phase_objects)]
                phase_success = sum(item.get("status") == "success" for item in phase_results)
                phase_failed = sum(item.get("status") == "failed" for item in phase_results)
                self.store.append_log(job_id, "INFO", f"阶段完成：{current_phase}，成功 {phase_success}，失败 {phase_failed}")
                with state_lock:
                    update_active()
            if stop_event.is_set() or self.store.cancelled(job_id):
                self.store.update(
                    job_id,
                    status="cancelled",
                    current_table=None,
                    finished_at=utc_now(),
                    table_results=table_results,
                    phase_progress=phase_progress_snapshot(),
                )
                self.store.append_log(job_id, "WARN", "任务已被用户取消")
            elif overall_failed:
                failed_names = [r["table"] for r in table_results if r["status"] == "failed"]
                self.store.update(job_id, status="failed", progress=round(completed / max(total_objects, 1) * 100, 2), current_table=None, finished_at=utc_now(), table_results=table_results, phase_progress=phase_progress_snapshot(), error=f"以下对象迁移失败：{', '.join(failed_names)}（失败策略：继续）")
                self.store.append_log(job_id, "ERROR", f"任务结束，以下对象迁移失败：{', '.join(failed_names)}")
            elif capture_for_later and start_scn is not None:
                self.store.update(
                    job_id,
                    status="completed",
                    progress=100,
                    sync_phase="ready_for_incremental",
                    current_table=None,
                    finished_at=utc_now(),
                    table_results=table_results,
                    phase_progress=phase_progress_snapshot(),
                )
                self.store.append_log(
                    job_id,
                    "INFO",
                    f"全量迁移成功，已保留起始 SCN={start_scn}；可在任务详情中手动启动增量同步",
                )
            elif replicator is not None and start_scn is not None:
                self.store.update(job_id, progress=100, table_results=table_results, phase_progress=phase_progress_snapshot())
                replicator.run(start_scn)
                if stop_event.is_set() or self.store.cancelled(job_id):
                    # finish-sync sets completed before signalling this worker.
                    # Preserve that normal terminal state; cancel still wins for
                    # explicitly aborted tasks.
                    if self.store.get(job_id)["status"] != "completed":
                        self.store.update(
                            job_id,
                            status="cancelled",
                            sync_phase="stopped",
                            current_table=None,
                            finished_at=utc_now(),
                        )
            else:
                self.store.update(job_id, status="completed", progress=100, current_table=None, finished_at=utc_now(), table_results=table_results, phase_progress=phase_progress_snapshot())
                self.store.append_log(job_id, "INFO", "任务全部完成")
        except Exception as exc:
            trace = traceback.format_exc()
            if stop_event.is_set() or self.store.cancelled(job_id):
                if self.store.get(job_id)["status"] != "completed":
                    self.store.update(
                        job_id,
                        status="cancelled",
                        current_table=None,
                        finished_at=utc_now(),
                    )
                    self.store.append_log(job_id, "WARN", "任务已被用户取消")
            else:
                logger.error("Migration %s failed: %s\n%s", job_id, exc, trace)
                self.store.update(job_id, status="failed", error=f"{exc}\n\n--- 完整异常堆栈 ---\n{trace}"[:12000], finished_at=utc_now())
                self.store.append_log(job_id, "ERROR", f"任务异常终止：{exc}")
        finally:
            if source_engine:
                source_engine.dispose()
            if target_engine:
                target_engine.dispose()
            if cdc_engine:
                cdc_engine.dispose()
            with self.lock:
                self.active.discard(job_id)
                self.stop_events.pop(job_id, None)
