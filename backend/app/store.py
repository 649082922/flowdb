from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from cryptography.fernet import Fernet


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def migration_object_total(payload: dict[str, Any]) -> int:
    """Count every selected migration object, including views and sequences."""
    table_objects = list(dict.fromkeys(payload.get("tables") or []))
    sequence_objects = (
        list(dict.fromkeys(payload.get("sequences") or []))
        if payload.get("migrate_sequences", True)
        else []
    )
    return len(table_objects) + len(sequence_objects)


class JobStore:
    def __init__(self, database_path: str, secret_key: str):
        self.path = Path(database_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.cipher = Fernet(secret_key.encode())
        self.lock = threading.RLock()
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    encrypted_payload BLOB NOT NULL,
                    status TEXT NOT NULL,
                    progress REAL NOT NULL DEFAULT 0,
                    rows_copied INTEGER NOT NULL DEFAULT 0,
                    bytes_copied INTEGER NOT NULL DEFAULT 0,
                    current_table TEXT,
                    tables_total INTEGER NOT NULL,
                    tables_completed INTEGER NOT NULL DEFAULT 0,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT
                )
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
            if "migration_content" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN migration_content TEXT NOT NULL DEFAULT 'structure_and_data'")
            if "batch_size" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN batch_size INTEGER NOT NULL DEFAULT 2000")
            if "table_concurrency" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN table_concurrency INTEGER NOT NULL DEFAULT 1")
            if "link_id" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN link_id TEXT")
            if "link_name" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN link_name TEXT")
            if "fail_policy" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN fail_policy TEXT NOT NULL DEFAULT 'stop_on_error'")
            if "table_results" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN table_results TEXT")
            if "phase_progress" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN phase_progress TEXT")
            if "identifier_case_policy" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN identifier_case_policy TEXT NOT NULL DEFAULT 'auto'")
            if "identifier_case_resolved" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN identifier_case_resolved TEXT NOT NULL DEFAULT 'preserve'")
            if "target_lower_case_table_names" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN target_lower_case_table_names INTEGER")
            cdc_columns = {
                "sync_mode": "TEXT NOT NULL DEFAULT 'full_only'",
                "sync_phase": "TEXT NOT NULL DEFAULT 'full'",
                "start_scn": "INTEGER",
                "checkpoint_scn": "INTEGER",
                "source_current_scn": "INTEGER",
                "cdc_lag": "INTEGER NOT NULL DEFAULT 0",
                "cdc_events": "INTEGER NOT NULL DEFAULT 0",
                "cdc_transactions": "INTEGER NOT NULL DEFAULT 0",
                "cdc_inserts": "INTEGER NOT NULL DEFAULT 0",
                "cdc_updates": "INTEGER NOT NULL DEFAULT 0",
                "cdc_deletes": "INTEGER NOT NULL DEFAULT 0",
                "cdc_started_at": "TEXT",
                "cdc_last_event_at": "TEXT",
            }
            for column_name, definition in cdc_columns.items():
                if column_name not in columns:
                    db.execute(f"ALTER TABLE jobs ADD COLUMN {column_name} {definition}")
            db.execute("CREATE INDEX IF NOT EXISTS jobs_created_idx ON jobs(created_at DESC)")
            db.execute("""
                CREATE TABLE IF NOT EXISTS migration_links (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    encrypted_payload BLOB NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_migration_links_name_unique ON migration_links(lower(name))")
            db.execute("""
                CREATE TABLE IF NOT EXISTS job_logs (
                    job_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    ts TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    PRIMARY KEY (job_id, seq)
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS job_logs_job_idx ON job_logs(job_id, seq)")
            db.execute("""
                CREATE TABLE IF NOT EXISTS cdc_applied_transactions (
                    job_id TEXT NOT NULL,
                    commit_scn INTEGER NOT NULL,
                    xid TEXT NOT NULL,
                    applied_at TEXT NOT NULL,
                    PRIMARY KEY (job_id, commit_scn, xid)
                )
            """)
            # Newer releases expose INSERT/UPDATE/DELETE counters separately.
            # Recover counters for older CDC jobs from their durable DML logs so
            # the UI does not misleadingly show 0/0/0 for already-run tasks.
            db.execute(
                """UPDATE jobs
                   SET cdc_inserts=(SELECT COUNT(*) FROM job_logs l
                                    WHERE l.job_id=jobs.id AND l.message LIKE '%][INSERT] %'),
                       cdc_updates=(SELECT COUNT(*) FROM job_logs l
                                    WHERE l.job_id=jobs.id AND l.message LIKE '%][UPDATE] %'),
                       cdc_deletes=(SELECT COUNT(*) FROM job_logs l
                                    WHERE l.job_id=jobs.id AND l.message LIKE '%][DELETE] %')
                   WHERE cdc_events > 0
                     AND cdc_inserts + cdc_updates + cdc_deletes = 0"""
            )
            db.execute("""
                CREATE TABLE IF NOT EXISTS validation_runs (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    passed INTEGER NOT NULL,
                    table_count INTEGER NOT NULL,
                    consistent_count INTEGER NOT NULL,
                    inconsistent_count INTEGER NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_validation_runs_created "
                "ON validation_runs(created_at DESC)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_validation_runs_status_created "
                "ON validation_runs(passed, created_at DESC)"
            )
            # A previous worker branch overwrote normally finished manual CDC
            # jobs as cancelled after ``finish-sync`` had marked them complete.
            # The durable normal-finish log distinguishes them from genuinely
            # cancelled tasks, so repair only those historical rows.
            db.execute(
                """UPDATE jobs
                   SET status='completed', tables_completed=tables_total
                   WHERE sync_mode='incremental_only'
                     AND status='cancelled'
                     AND sync_phase='stopped'
                     AND EXISTS (
                         SELECT 1 FROM job_logs l
                         WHERE l.job_id=jobs.id
                           AND l.message LIKE '用户正常结束实时同步%'
                     )"""
            )
            # 早期版本只把 payload.tables 计入对象总数，导致纯序列任务显示
            # 0/0、带序列任务的总数偏小。启动时安全回填历史任务。
            for row in db.execute(
                "SELECT id, encrypted_payload, tables_total, tables_completed, table_results, status, sync_mode FROM jobs"
            ).fetchall():
                try:
                    payload = json.loads(self.cipher.decrypt(row[1]).decode())
                    expected_total = migration_object_total(payload)
                    results = json.loads(row[4]) if row[4] else []
                    expected_completed = sum(
                        item.get("status") == "success" for item in results
                    )
                    if row[6] == "incremental_only" and row[5] in {
                        "running", "catching_up", "syncing", "completed", "cancelled"
                    }:
                        # Incremental tasks monitor the selected set; they do
                        # not re-run full object migration and therefore have
                        # no table_results entries of their own.
                        expected_completed = expected_total
                except Exception:
                    continue
                if (
                    int(row[2]) != expected_total
                    or int(row[3]) != expected_completed
                ):
                    db.execute(
                        "UPDATE jobs SET tables_total=?, tables_completed=? WHERE id=?",
                        (expected_total, expected_completed, row[0]),
                    )
            db.execute("PRAGMA optimize")

    def create(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        encrypted = self.cipher.encrypt(json.dumps(payload).encode())
        now = utc_now()
        with self.lock, self._connect() as db:
            db.execute(
                """INSERT INTO jobs
                   (id,name,source_type,target_type,encrypted_payload,status,tables_total,created_at,migration_content,batch_size,table_concurrency,link_id,link_name,fail_policy,identifier_case_policy,identifier_case_resolved,target_lower_case_table_names,sync_mode,sync_phase,start_scn,checkpoint_scn)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (job_id, payload["name"], payload["source"]["type"], payload["target"]["type"], encrypted, "queued", migration_object_total(payload), now, payload.get("migration_content", "structure_and_data"), payload.get("batch_size", 2000), payload.get("table_concurrency", 1), payload.get("link_id"), payload.get("link_name"), payload.get("fail_policy", "stop_on_error"), payload.get("identifier_case_policy", "auto"), payload.get("identifier_case_resolved", "preserve"), payload.get("target_lower_case_table_names"), payload.get("sync_mode", "full_only"), "incremental" if payload.get("sync_mode") == "incremental_only" else "full", payload.get("start_scn"), payload.get("start_scn")),
            )
        return self.get(job_id)

    def get_payload(self, job_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT encrypted_payload FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            raise KeyError(job_id)
        return json.loads(self.cipher.decrypt(row[0]).decode())

    def get(self, job_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            raise KeyError(job_id)
        return self._public(dict(row))

    def list(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        return [self._public(dict(row)) for row in rows]

    def count(self) -> int:
        with self._connect() as db:
            return int(db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0])

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        row.pop("encrypted_payload", None)
        row.pop("cancel_requested", None)
        results = row.get("table_results")
        row["table_results"] = json.loads(results) if results else []
        phases = row.get("phase_progress")
        row["phase_progress"] = json.loads(phases) if phases else []
        row["fail_policy"] = row.get("fail_policy") or "stop_on_error"
        row["identifier_case_policy"] = row.get("identifier_case_policy") or "auto"
        row["identifier_case_resolved"] = row.get("identifier_case_resolved") or "preserve"
        row["sync_mode"] = row.get("sync_mode") or "full_only"
        row["sync_phase"] = row.get("sync_phase") or "full"
        row["cdc_lag"] = int(row.get("cdc_lag") or 0)
        row["cdc_events"] = int(row.get("cdc_events") or 0)
        row["cdc_transactions"] = int(row.get("cdc_transactions") or 0)
        row["cdc_inserts"] = int(row.get("cdc_inserts") or 0)
        row["cdc_updates"] = int(row.get("cdc_updates") or 0)
        row["cdc_deletes"] = int(row.get("cdc_deletes") or 0)
        return row

    def update(self, job_id: str, **values: Any) -> None:
        allowed = {"status", "progress", "rows_copied", "bytes_copied", "current_table", "tables_completed", "cancel_requested", "error", "started_at", "finished_at", "fail_policy", "table_results", "phase_progress", "sync_phase", "start_scn", "checkpoint_scn", "source_current_scn", "cdc_lag", "cdc_events", "cdc_transactions", "cdc_inserts", "cdc_updates", "cdc_deletes", "cdc_started_at", "cdc_last_event_at"}
        safe = {key: value for key, value in values.items() if key in allowed}
        if not safe:
            return
        assignments = ",".join(f"{key}=?" for key in safe)
        params: list[Any] = []
        for key, value in safe.items():
            if key in {"table_results", "phase_progress"}:
                value = json.dumps(value, ensure_ascii=False)
            params.append(value)
        params.append(job_id)
        with self.lock, self._connect() as db:
            db.execute(f"UPDATE jobs SET {assignments} WHERE id=?", (*params,))

    def cancelled(self, job_id: str) -> bool:
        with self._connect() as db:
            row = db.execute("SELECT cancel_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
        return bool(row and row[0])

    def append_log(self, job_id: str, level: str, message: str) -> None:
        with self.lock, self._connect() as db:
            row = db.execute("SELECT COALESCE(MAX(seq), 0) AS seq FROM job_logs WHERE job_id=?", (job_id,)).fetchone()
            db.execute(
                "INSERT INTO job_logs (job_id, seq, ts, level, message) VALUES (?,?,?,?,?)",
                (job_id, int(row["seq"]) + 1, utc_now(), level, message[:2000]),
            )

    def get_logs(self, job_id: str, after_seq: int = 0, limit: int = 2000) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT seq, ts, level, message FROM job_logs WHERE job_id=? AND seq > ? ORDER BY seq LIMIT ?",
                (job_id, after_seq, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def cdc_transaction_applied(self, job_id: str, commit_scn: int, xid: str) -> bool:
        with self._connect() as db:
            row = db.execute(
                "SELECT 1 FROM cdc_applied_transactions WHERE job_id=? AND commit_scn=? AND xid=?",
                (job_id, int(commit_scn), str(xid)),
            ).fetchone()
        return row is not None

    def record_cdc_transaction(self, job_id: str, commit_scn: int, xid: str) -> None:
        with self.lock, self._connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO cdc_applied_transactions(job_id,commit_scn,xid,applied_at) VALUES (?,?,?,?)",
                (job_id, int(commit_scn), str(xid), utc_now()),
            )

    def create_validation(
        self,
        validation_id: str,
        job_id: str,
        job_name: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        tables = result.get("tables") or []
        consistent_count = sum(bool(item.get("passed")) for item in tables)
        created_at = utc_now()
        with self.lock, self._connect() as db:
            db.execute(
                """INSERT INTO validation_runs
                   (id,job_id,job_name,passed,table_count,consistent_count,
                    inconsistent_count,result_json,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    validation_id,
                    job_id,
                    job_name,
                    int(bool(result.get("passed"))),
                    len(tables),
                    consistent_count,
                    len(tables) - consistent_count,
                    json.dumps(result, ensure_ascii=False),
                    created_at,
                ),
            )
        return self.get_validation(validation_id)

    def get_validation(self, validation_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute(
                "SELECT * FROM validation_runs WHERE id=?", (validation_id,)
            ).fetchone()
        if not row:
            raise KeyError(validation_id)
        item = dict(row)
        result = json.loads(item.pop("result_json"))
        item["passed"] = bool(item["passed"])
        return {**result, **item}

    def list_validations(
        self, status: str = "all", limit: int = 10, offset: int = 0
    ) -> list[dict[str, Any]]:
        where = ""
        params: list[Any] = []
        if status in {"passed", "failed"}:
            where = "WHERE passed=?"
            params.append(1 if status == "passed" else 0)
        params.extend((limit, offset))
        with self._connect() as db:
            rows = db.execute(
                f"""SELECT id,job_id,job_name,passed,table_count,
                           consistent_count,inconsistent_count,created_at
                    FROM validation_runs {where}
                    ORDER BY created_at DESC LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        return [
            {**dict(row), "passed": bool(row["passed"])} for row in rows
        ]

    def count_validations(self, status: str = "all") -> int:
        where = ""
        params: tuple[int, ...] = ()
        if status in {"passed", "failed"}:
            where = "WHERE passed=?"
            params = (1 if status == "passed" else 0,)
        with self._connect() as db:
            return int(
                db.execute(
                    f"SELECT COUNT(*) FROM validation_runs {where}", params
                ).fetchone()[0]
            )

    def create_link(self, link_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = utc_now()
        encrypted = self.cipher.encrypt(json.dumps(payload).encode())
        try:
            with self.lock, self._connect() as db:
                db.execute(
                    "INSERT INTO migration_links (id,name,source_type,target_type,encrypted_payload,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
                    (link_id, payload["name"].strip(), payload["source"]["type"], payload["target"]["type"], encrypted, now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError("链路名称已存在，请使用其他名称") from exc
        return self.get_link(link_id)

    def update_link(self, link_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        encrypted = self.cipher.encrypt(json.dumps(payload).encode())
        try:
            with self.lock, self._connect() as db:
                cursor = db.execute(
                    "UPDATE migration_links SET name=?,source_type=?,target_type=?,encrypted_payload=?,updated_at=? WHERE id=?",
                    (payload["name"].strip(), payload["source"]["type"], payload["target"]["type"], encrypted, utc_now(), link_id),
                )
                if cursor.rowcount == 0:
                    raise KeyError(link_id)
        except sqlite3.IntegrityError as exc:
            raise ValueError("链路名称已存在，请使用其他名称") from exc
        return self.get_link(link_id)

    def delete_link(self, link_id: str) -> None:
        with self.lock, self._connect() as db:
            cursor = db.execute("DELETE FROM migration_links WHERE id=?", (link_id,))
            if cursor.rowcount == 0:
                raise KeyError(link_id)

    def get_link_payload(self, link_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT encrypted_payload FROM migration_links WHERE id=?", (link_id,)).fetchone()
        if not row:
            raise KeyError(link_id)
        return json.loads(self.cipher.decrypt(row[0]).decode())

    def get_link(self, link_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM migration_links WHERE id=?", (link_id,)).fetchone()
        if not row:
            raise KeyError(link_id)
        return self._public_link(dict(row))

    def list_links(self) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM migration_links ORDER BY updated_at DESC").fetchall()
        return [self._public_link(dict(row)) for row in rows]

    def _public_link(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = json.loads(self.cipher.decrypt(row.pop("encrypted_payload")).decode())
        def summary(config: dict[str, Any]) -> dict[str, Any]:
            return {key: value for key, value in config.items() if key != "password"} | {"has_password": bool(config.get("password"))}
        return {"id": row["id"], "name": row["name"], "source": summary(payload["source"]), "target": summary(payload["target"]), "created_at": row["created_at"], "updated_at": row["updated_at"]}


def build_store() -> JobStore:
    secret = os.environ.get("FLOWDB_SECRET_KEY")
    if not secret:
        raise RuntimeError("必须设置 FLOWDB_SECRET_KEY（可使用 Fernet.generate_key() 生成）")
    return JobStore(os.environ.get("FLOWDB_STATE_DB", "/data/flowdb.sqlite3"), secret)
