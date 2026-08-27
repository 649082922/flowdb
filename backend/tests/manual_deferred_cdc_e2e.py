"""Manual E2E: successful full baseline, then separately started incremental task."""
from __future__ import annotations

import json
import os
import time
import urllib.request

from sqlalchemy import text

from app.database import make_engine
from app.models import ConnectionConfig
from app.store import build_store

API = "http://127.0.0.1:8000"
TABLE = "FLOWDB_DEFER_0826"


def api(path: str, payload: dict | None = None):
    request = urllib.request.Request(
        API + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        method="POST" if payload is not None else "GET",
        headers={"content-type": "application/json", "x-flowdb-token": os.environ["FLOWDB_API_TOKEN"]},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def wait_job(job_id: str, predicate, timeout: int = 180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = api(f"/api/jobs/{job_id}")
        if job["status"] == "failed":
            raise RuntimeError(job.get("error"))
        if predicate(job):
            return job
        time.sleep(0.5)
    raise TimeoutError(api(f"/api/jobs/{job_id}"))


def main(link_id: str):
    link = build_store().get_link_payload(link_id)
    source = make_engine(ConnectionConfig.model_validate(link["source"]))
    target = make_engine(ConnectionConfig.model_validate(link["target"]))
    with source.begin() as connection:
        connection.exec_driver_sql(
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {TABLE} PURGE'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        connection.exec_driver_sql(
            f"CREATE TABLE {TABLE} (ID NUMBER(12) PRIMARY KEY, NOTE VARCHAR2(100), AMOUNT NUMBER(20,4))"
        )
        connection.execute(
            text(f"INSERT INTO {TABLE} (ID,NOTE,AMOUNT) VALUES (:id,:note,:amount)"),
            [{"id": i, "note": f"baseline-{i}", "amount": i / 10} for i in range(1, 21)],
        )

    full = api(
        "/api/jobs",
        {
            "name": "QA-先全量后独立增量",
            "link_id": link_id,
            "tables": [TABLE],
            "object_types": {TABLE: "table"},
            "batch_size": 100,
            "table_concurrency": 1,
            "existing_table": "drop_and_create",
            "migration_content": "structure_and_data",
            "fail_policy": "stop_on_error",
            "migrate_sequences": False,
            "sync_mode": "full_then_incremental",
            "cdc_poll_seconds": 1,
            "cdc_window_scn": 50000,
            "cdc_no_key_policy": "reject",
        },
    )
    full = wait_job(
        full["id"],
        lambda job: job["status"] == "completed" and job["sync_phase"] == "ready_for_incremental",
    )
    if not full.get("start_scn") or full.get("rows_copied") != 20:
        raise AssertionError(full)

    # These changes happen after the full baseline but before the user starts CDC.
    with source.begin() as connection:
        connection.execute(text(f"UPDATE {TABLE} SET NOTE='between-full-and-cdc' WHERE ID=1"))
        connection.execute(text(f"DELETE FROM {TABLE} WHERE ID=2"))
        connection.execute(text(f"INSERT INTO {TABLE} VALUES (21,'between-insert',2.1)"))

    incremental = api(f"/api/jobs/{full['id']}/start-incremental", {})
    incremental = wait_job(
        incremental["id"],
        lambda job: job["status"] == "syncing" and int(job.get("cdc_lag") or 0) == 0,
    )

    # Continue producing data after catch-up to prove the task remains real-time.
    with source.begin() as connection:
        connection.execute(text(f"UPDATE {TABLE} SET NOTE='after-realtime' WHERE ID=3"))
        connection.execute(text(f"INSERT INTO {TABLE} VALUES (22,'after-insert',2.2)"))

    incremental = wait_job(
        incremental["id"],
        lambda job: int(job.get("cdc_events") or 0) >= 5 and int(job.get("cdc_lag") or 0) == 0,
    )
    with target.connect() as connection:
        target_name = TABLE.lower()
        result = {
            "count": int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name}")).scalar_one()),
            "id1": connection.execute(text(f"SELECT NOTE FROM {target_name} WHERE ID=1")).scalar_one(),
            "id2": int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name} WHERE ID=2")).scalar_one()),
            "id3": connection.execute(text(f"SELECT NOTE FROM {target_name} WHERE ID=3")).scalar_one(),
            "id21": int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name} WHERE ID=21")).scalar_one()),
            "id22": int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name} WHERE ID=22")).scalar_one()),
        }
    expected = {"count": 21, "id1": "between-full-and-cdc", "id2": 0, "id3": "after-realtime", "id21": 1, "id22": 1}
    if result != expected:
        raise AssertionError({"result": result, "expected": expected})

    api(f"/api/jobs/{incremental['id']}/finish-sync", {})
    terminal = wait_job(incremental["id"], lambda job: job["status"] == "completed")
    print(json.dumps({
        "ok": True,
        "full_job_id": full["id"],
        "incremental_job_id": terminal["id"],
        "saved_start_scn": full["start_scn"],
        "full_rows": full["rows_copied"],
        "cdc_events": terminal["cdc_events"],
        "cdc_inserts": terminal.get("cdc_inserts"),
        "cdc_updates": terminal.get("cdc_updates"),
        "cdc_deletes": terminal.get("cdc_deletes"),
        "target": result,
        "terminal": terminal["status"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    import sys
    main(sys.argv[1])
