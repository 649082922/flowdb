"""Authorized 196-only full + LogMiner incremental end-to-end probe."""

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
TABLE = "FLOWDB_CDC_E2E_0824"


def api(path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        API + path,
        data=data,
        method="POST" if payload is not None else "GET",
        headers={
            "content-type": "application/json",
            "x-flowdb-token": os.environ["FLOWDB_API_TOKEN"],
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def wait_for(job_id: str, accepted: set[str], timeout: int = 180) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = api(f"/api/jobs/{job_id}")
        if job["status"] in accepted:
            return job
        if job["status"] == "failed":
            raise RuntimeError(job.get("error") or "CDC job failed")
        time.sleep(1)
    raise TimeoutError(f"job {job_id} did not reach {accepted}")


def rows(engine, table_name: str) -> list[tuple]:
    with engine.connect() as connection:
        result = connection.execute(
            text(f"SELECT ID, NAME, AMOUNT, NOTE FROM {table_name} ORDER BY ID")
        )
        return [tuple(row) for row in result]


def main(link_id: str) -> None:
    link = build_store().get_link_payload(link_id)
    source_engine = make_engine(ConnectionConfig.model_validate(link["source"]))
    target_engine = make_engine(ConnectionConfig.model_validate(link["target"]))
    with source_engine.begin() as connection:
        try:
            connection.execute(text(f"DROP TABLE {TABLE} PURGE"))
        except Exception:
            pass
        connection.execute(
            text(
                f"CREATE TABLE {TABLE} ("
                "ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(100), "
                "AMOUNT NUMBER(20,4), CHANGED_AT TIMESTAMP(6), NOTE CLOB)"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {TABLE}(ID,NAME,AMOUNT,CHANGED_AT,NOTE) "
                "VALUES (1,'全量一',12.3400,SYSTIMESTAMP,'初始 CLOB 一')"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {TABLE}(ID,NAME,AMOUNT,CHANGED_AT,NOTE) "
                "VALUES (2,'待删除',99.0001,SYSTIMESTAMP,NULL)"
            )
        )

    job = api(
        "/api/jobs",
        {
            "name": "QA-Oracle-LogMiner-全量增量闭环",
            "link_id": link_id,
            "tables": [TABLE],
            "object_types": {TABLE: "table"},
            "batch_size": 100,
            "table_concurrency": 1,
            "existing_table": "drop_and_create",
            "migration_content": "structure_and_data",
            "fail_policy": "stop_on_error",
            "migrate_sequences": False,
            "sync_mode": "full_and_incremental",
            "cdc_poll_seconds": 1,
            "cdc_window_scn": 10000,
        },
    )
    job_id = job["id"]
    wait_for(job_id, {"syncing"})
    full_rows = rows(target_engine, TABLE.lower())
    if len(full_rows) != 2:
        raise AssertionError(f"full copy mismatch: {full_rows!r}")

    with source_engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {TABLE} SET NAME='增量更新', AMOUNT=12345.6789, "
                "CHANGED_AT=SYSTIMESTAMP, NOTE='中文 CLOB 已由 LogMiner 同步' WHERE ID=1"
            )
        )
        connection.execute(text(f"DELETE FROM {TABLE} WHERE ID=2"))
        connection.execute(
            text(
                f"INSERT INTO {TABLE}(ID,NAME,AMOUNT,CHANGED_AT,NOTE) "
                "VALUES (3,'增量新增',0.0001,SYSTIMESTAMP,NULL)"
            )
        )

    deadline = time.monotonic() + 120
    actual: list[tuple] = []
    while time.monotonic() < deadline:
        actual = rows(target_engine, TABLE.lower())
        if [row[0] for row in actual] == [1, 3] and actual[0][1] == "增量更新":
            break
        job = api(f"/api/jobs/{job_id}")
        if job["status"] == "failed":
            raise RuntimeError(job.get("error") or "CDC job failed")
        time.sleep(1)
    else:
        raise AssertionError(f"incremental copy mismatch: {actual!r}")

    final_job = api(f"/api/jobs/{job_id}")
    api(f"/api/jobs/{job_id}/cancel", {})
    print(
        {
            "job_id": job_id,
            "full_rows": len(full_rows),
            "final_ids": [row[0] for row in actual],
            "updated_name": actual[0][1],
            "updated_amount": str(actual[0][2]),
            "updated_clob": actual[0][3],
            "checkpoint_scn": final_job.get("checkpoint_scn"),
            "cdc_events": final_job.get("cdc_events"),
            "cdc_transactions": final_job.get("cdc_transactions"),
            "lag_scn": final_job.get("lag_scn"),
        }
    )
    source_engine.dispose()
    target_engine.dispose()


if __name__ == "__main__":
    import sys

    main(sys.argv[1])
