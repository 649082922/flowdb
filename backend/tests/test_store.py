import tempfile
import unittest
from pathlib import Path

from cryptography.fernet import Fernet

from app.store import JobStore


class ValidationHistoryStoreTests(unittest.TestCase):
    def test_cdc_transaction_deduplication_is_durable(self):
        self.assertFalse(self.store.cdc_transaction_applied("job-1", 12345, "xid-1"))
        self.store.record_cdc_transaction("job-1", 12345, "xid-1")
        self.store.record_cdc_transaction("job-1", 12345, "xid-1")
        self.assertTrue(self.store.cdc_transaction_applied("job-1", 12345, "xid-1"))

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = str(Path(self.temp_dir.name) / "flowdb-test.sqlite3")
        self.secret = Fernet.generate_key().decode()
        self.store = JobStore(self.database_path, self.secret)

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def result(passed: bool) -> dict:
        return {
            "passed": passed,
            "max_hash_rows": 100000,
            "tables": [
                {"table": "T_OK", "passed": True},
                {"table": "T_DIFF", "passed": passed},
            ],
        }

    def test_validation_records_persist_and_filter(self):
        self.store.create_validation(
            "validation-pass", "job-1", "全量迁移", self.result(True)
        )
        self.store.create_validation(
            "validation-fail", "job-1", "全量迁移", self.result(False)
        )

        self.assertEqual(self.store.count_validations(), 2)
        self.assertEqual(self.store.count_validations("passed"), 1)
        self.assertEqual(self.store.count_validations("failed"), 1)
        self.assertEqual(
            self.store.list_validations("failed")[0]["id"], "validation-fail"
        )

        reopened = JobStore(self.database_path, self.secret)
        detail = reopened.get_validation("validation-fail")
        self.assertFalse(detail["passed"])
        self.assertEqual(detail["table_count"], 2)
        self.assertEqual(detail["consistent_count"], 1)
        self.assertEqual(detail["inconsistent_count"], 1)
        self.assertEqual(detail["tables"][1]["table"], "T_DIFF")

    def test_job_total_counts_tables_views_and_sequences_and_backfills(self):
        payload = {
            "name": "对象计数测试",
            "source": {
                "type": "oracle",
                "host": "source",
                "port": 1521,
                "database": "pdb01",
                "username": "source_user",
                "password": "source_password",
            },
            "target": {
                "type": "mysql",
                "host": "target",
                "port": 3306,
                "database": "test",
                "username": "target_user",
                "password": "target_password",
            },
            "tables": ["NORMAL_TABLE", "FINAL_VIEW"],
            "object_types": {"NORMAL_TABLE": "table", "FINAL_VIEW": "view"},
            "sequences": ["SOURCE_SEQUENCE"],
            "migrate_sequences": True,
        }
        created = self.store.create("job-object-total", payload)
        self.assertEqual(created["tables_total"], 3)
        self.assertEqual(created["sync_mode"], "full_only")

        with self.store._connect() as db:
            db.execute(
                "UPDATE jobs SET tables_total=0, tables_completed=0, "
                "table_results=? WHERE id='job-object-total'",
                ('[{"table":"SOURCE_SEQUENCE","status":"success"}]',),
            )
        reopened = JobStore(self.database_path, self.secret)
        self.assertEqual(reopened.get("job-object-total")["tables_total"], 3)
        self.assertEqual(reopened.get("job-object-total")["tables_completed"], 1)

    def test_incremental_checkpoint_fields_are_durable(self):
        payload = {
            "name": "全量增量测试",
            "source": {"type": "oracle"},
            "target": {"type": "tdsql"},
            "tables": ["CLX.T1"],
            "sync_mode": "full_and_incremental",
        }
        created = self.store.create("job-cdc", payload)
        self.assertEqual(created["sync_mode"], "full_and_incremental")
        self.store.update(
            "job-cdc",
            status="syncing",
            sync_phase="realtime",
            start_scn=100,
            checkpoint_scn=150,
            source_current_scn=155,
            cdc_lag=5,
            cdc_events=8,
            cdc_transactions=3,
        )
        reopened = JobStore(self.database_path, self.secret).get("job-cdc")
        self.assertEqual(reopened["checkpoint_scn"], 150)
        self.assertEqual(reopened["cdc_lag"], 5)
        self.assertEqual(reopened["cdc_events"], 8)

    def test_normal_finished_incremental_job_is_repaired_and_counts_monitored_objects(self):
        payload = {
            "name": "手动增量",
            "source": {"type": "oracle"},
            "target": {"type": "tdsql"},
            "tables": ["CLX.T1", "CLX.T2"],
            "sync_mode": "incremental_only",
            "start_scn": 100,
        }
        self.store.create("job-normal-finish", payload)
        self.store.update(
            "job-normal-finish",
            status="cancelled",
            sync_phase="stopped",
            checkpoint_scn=150,
        )
        self.store.append_log(
            "job-normal-finish",
            "INFO",
            "用户正常结束实时同步，任务已完成：最终检查点 SCN=150",
        )

        reopened = JobStore(self.database_path, self.secret).get("job-normal-finish")
        self.assertEqual(reopened["status"], "completed")
        self.assertEqual(reopened["tables_completed"], 2)
        self.assertEqual(reopened["tables_total"], 2)

    def test_explicitly_cancelled_incremental_job_stays_cancelled(self):
        payload = {
            "name": "手动增量取消",
            "source": {"type": "oracle"},
            "target": {"type": "tdsql"},
            "tables": ["CLX.T1"],
            "sync_mode": "incremental_only",
            "start_scn": 100,
        }
        self.store.create("job-explicit-cancel", payload)
        self.store.update(
            "job-explicit-cancel",
            status="cancelled",
            sync_phase="stopped",
            checkpoint_scn=120,
        )
        self.store.append_log("job-explicit-cancel", "WARN", "用户确认取消，任务已停止")

        reopened = JobStore(self.database_path, self.secret).get("job-explicit-cancel")
        self.assertEqual(reopened["status"], "cancelled")
        self.assertEqual(reopened["tables_completed"], 1)

if __name__ == "__main__":
    unittest.main()
