import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from app.models import ConnectionConfig
from app.worker import (
    _build_tdsql_sequence_ddl,
    _execute_tdsql_sequence_ddl,
    _target_supports_tdsql_sequences,
    MigrationRunner,
    migration_error_message,
    ordered_migration_phases,
)


class _VersionResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class _VersionConnection:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _statement):
        return _VersionResult(self.row)


def _engine(version: str, comment: str = ""):
    return SimpleNamespace(
        connect=lambda: _VersionConnection(
            {"version": version, "version_comment": comment}
        )
    )


def _config(db_type: str = "mysql") -> ConnectionConfig:
    return ConnectionConfig(
        type=db_type,
        host="127.0.0.1",
        port=3306,
        database="test",
        username="test",
        password="test",
    )


class SequencePlanningTests(unittest.TestCase):
    def test_migration_error_keeps_type_and_database_detail(self):
        error = migration_error_message(
            RuntimeError("(3675, Create table failed, as disk is full)"),
            "QA_VIEW",
        )
        self.assertIn("目标端空间", error)
        self.assertIn("RuntimeError", error)
        self.assertIn("QA_VIEW", error)
        self.assertIn("3675", error)

    def test_mysql_compatible_txsql_is_detected(self):
        supported, detail = _target_supports_tdsql_sequences(
            _engine("8.0.33-v24-txsql-22.4.1-20230926"), _config()
        )
        self.assertTrue(supported)
        self.assertIn("txsql", detail.lower())

    def test_standard_mysql_is_not_mistaken_for_tdsql(self):
        supported, detail = _target_supports_tdsql_sequences(
            _engine("8.0.36", "MySQL Community Server - GPL"), _config()
        )
        self.assertFalse(supported)
        self.assertIn("不是支持", detail)

    def test_sequence_phase_precedes_tables(self):
        phases = ordered_migration_phases(
            ["NORMAL_TABLE", "PART_TABLE", "FINAL_VIEW"],
            {
                "NORMAL_TABLE": "table",
                "PART_TABLE": "partitioned_table",
                "FINAL_VIEW": "view",
            },
            ["SOURCE_SEQUENCE"],
        )
        self.assertEqual(
            [label for label, _objects in phases],
            ["序列迁移", "普通表迁移", "分区表迁移", "视图迁移"],
        )
        self.assertEqual(phases[0][1], ["SOURCE_SEQUENCE"])

    def test_tdsql_sequence_identifier_is_quoted(self):
        ddl = _build_tdsql_sequence_ddl(
            "ISEQ$$_76238",
            {
                "last_number": 10,
                "increment_by": 1,
                "min_value": 1,
                "max_value": 1000,
                "cache_size": 20,
                "cycle_flag": "N",
            },
        )
        self.assertTrue(ddl.startswith("CREATE TDSQL_SEQUENCE `ISEQ$$_76238` "))


class MigrationCancellationTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.runner = MigrationRunner(self.store, workers=1)

    def tearDown(self):
        self.runner.pool.shutdown(wait=True, cancel_futures=True)

    def test_request_cancel_signals_active_worker(self):
        job_id = "cancel-active-job"
        self.runner.stop_events[job_id] = threading.Event()
        self.assertTrue(self.runner.request_cancel(job_id))
        self.assertTrue(self.runner.stop_events[job_id].is_set())

    def test_cancelled_job_does_not_open_database_connections(self):
        job_id = "already-cancelled-job"
        self.store.cancelled.return_value = True

        self.runner._run(job_id)

        self.store.get_payload.assert_not_called()
        self.store.update.assert_called_once()
        self.assertEqual(self.store.update.call_args.kwargs["status"], "cancelled")


class _ScalarResult:
    def scalar_one(self):
        return 321


class _BlockingSequenceConnection:
    def __init__(self, killed: threading.Event, control: bool = False):
        self.killed = killed
        self.control = control

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement):
        sql = str(statement)
        if "CONNECTION_ID" in sql:
            return _ScalarResult()
        if sql.startswith("KILL "):
            self.killed.set()
            return None
        if "lock_wait_timeout" in sql:
            return None
        self.killed.wait(1)
        return None

    def commit(self):
        return None


class _BlockingSequenceEngine:
    def __init__(self):
        self.killed = threading.Event()

    def connect(self):
        return _BlockingSequenceConnection(self.killed)

    def begin(self):
        return _BlockingSequenceConnection(self.killed, control=True)


class SequenceTimeoutTests(unittest.TestCase):
    def test_tdsql_sequence_timeout_kills_connection_and_returns(self):
        engine = _BlockingSequenceEngine()
        started = __import__("time").perf_counter()
        with self.assertRaises(TimeoutError):
            _execute_tdsql_sequence_ddl(
                engine,
                ["DROP TDSQL_SEQUENCE blocked_sequence"],
                timeout_seconds=0.05,
            )
        self.assertTrue(engine.killed.is_set())
        self.assertLess(__import__("time").perf_counter() - started, 0.5)


if __name__ == "__main__":
    unittest.main()
