"""Authorized 196-only Oracle -> TDSQL no-primary-key CDC end-to-end probe."""

from __future__ import annotations

import json
import os
import time
import urllib.request
from decimal import Decimal

from sqlalchemy import text

from app.database import make_engine
from app.models import ConnectionConfig
from app.store import build_store


API = "http://127.0.0.1:8000"
PREFIX = "FLOWDB_NPK_0825"


def api(path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        API + path,
        data=data,
        method="POST" if payload is not None else "GET",
        headers={"content-type": "application/json", "x-flowdb-token": os.environ["FLOWDB_API_TOKEN"]},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def create_job(link_id: str, name: str, table_name: str, **cdc: object) -> str:
    result = api(
        "/api/jobs",
        {
            "name": name,
            "link_id": link_id,
            "tables": [table_name],
            "object_types": {table_name: "table"},
            "batch_size": 100,
            "table_concurrency": 1,
            "existing_table": "drop_and_create",
            "migration_content": "structure_and_data",
            "fail_policy": "stop_on_error",
            "migrate_sequences": False,
            "sync_mode": "full_and_incremental",
            "cdc_poll_seconds": 1,
            "cdc_window_scn": 10000,
            **cdc,
        },
    )
    return result["id"]


def wait_status(job_id: str, accepted: set[str], timeout: int = 180) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = api(f"/api/jobs/{job_id}")
        if job["status"] in accepted:
            return job
        if job["status"] == "failed" and "failed" not in accepted:
            raise RuntimeError(job.get("error") or f"job {job_id} failed")
        time.sleep(1)
    raise TimeoutError(f"job {job_id} did not reach {accepted}")


def target_rows(engine, table_name: str, columns: str, order_by: str) -> list[tuple]:
    with engine.connect() as connection:
        return [
            tuple(row)
            for row in connection.execute(
                text(f"SELECT {columns} FROM {table_name.lower()} ORDER BY {order_by}")
            )
        ]


def wait_rows(engine, table_name: str, columns: str, order_by: str, expected: list[tuple]) -> None:
    deadline = time.monotonic() + 120
    actual: list[tuple] = []
    while time.monotonic() < deadline:
        actual = target_rows(engine, table_name, columns, order_by)
        if actual == expected:
            return
        time.sleep(1)
    raise AssertionError(f"{table_name} mismatch: {actual!r} != {expected!r}")


def main(link_id: str) -> None:
    link = build_store().get_link_payload(link_id)
    source = make_engine(ConnectionConfig.model_validate(link["source"]))
    target = make_engine(ConnectionConfig.model_validate(link["target"]))
    names = {
        "unique": f"{PREFIX}_UK",
        "business": f"{PREFIX}_BK",
        "all": f"{PREFIX}_ALL",
        "none": f"{PREFIX}_NONE",
        "duplicate": f"{PREFIX}_DUP",
        "null": f"{PREFIX}_NULL",
    }
    for name in names.values():
        with source.begin() as connection:
            try:
                connection.execute(text(f"DROP TABLE {name} PURGE"))
            except Exception:
                pass
    with source.begin() as connection:
        connection.execute(text(f"CREATE TABLE {names['unique']} (CODE VARCHAR2(40) NOT NULL UNIQUE, QTY NUMBER(12,2), NOTE CLOB)"))
        connection.execute(text(f"INSERT INTO {names['unique']} VALUES ('U1',1.25,'唯一键初始一')"))
        connection.execute(text(f"INSERT INTO {names['unique']} VALUES ('U2',2.50,NULL)"))
        connection.execute(text(f"CREATE TABLE {names['business']} (TENANT_ID NUMBER(8), ORDER_NO VARCHAR2(40), QTY NUMBER(12,2), NOTE CLOB)"))
        connection.execute(text(f"INSERT INTO {names['business']} VALUES (1,'O1',10.00,'业务键初始一')"))
        connection.execute(text(f"INSERT INTO {names['business']} VALUES (1,'O2',20.00,NULL)"))
        connection.execute(text(f"CREATE TABLE {names['all']} (CODE VARCHAR2(40), QTY NUMBER(12,2), NOTE CLOB)"))
        connection.execute(text(f"INSERT INTO {names['all']} VALUES ('A',1.00,'ALL 初始一')"))
        connection.execute(text(f"INSERT INTO {names['all']} VALUES ('B',2.00,NULL)"))
        connection.execute(text(f"CREATE TABLE {names['none']} (CODE VARCHAR2(40), NOTE CLOB)"))
        connection.execute(text(f"INSERT INTO {names['none']} VALUES ('N1','无键拒绝')"))
        connection.execute(text(f"CREATE TABLE {names['duplicate']} (CODE VARCHAR2(40), NOTE CLOB)"))
        connection.execute(text(f"INSERT INTO {names['duplicate']} VALUES ('DUP','一')"))
        connection.execute(text(f"INSERT INTO {names['duplicate']} VALUES ('DUP','二')"))
        connection.execute(text(f"CREATE TABLE {names['null']} (CODE VARCHAR2(40), NOTE CLOB)"))
        connection.execute(text(f"INSERT INTO {names['null']} VALUES (NULL,'空业务键')"))

    results: dict[str, object] = {}

    unique_job = create_job(link_id, "QA-无主键-自动唯一键", names["unique"])
    wait_status(unique_job, {"syncing"})
    with source.begin() as connection:
        connection.execute(text(f"UPDATE {names['unique']} SET CODE='U1X', QTY=9.75, NOTE='唯一键增量中文' WHERE CODE='U1'"))
        connection.execute(text(f"DELETE FROM {names['unique']} WHERE CODE='U2'"))
        connection.execute(text(f"INSERT INTO {names['unique']} VALUES ('U3',3.33,NULL)"))
    wait_rows(target, names["unique"], "CODE,QTY,NOTE", "CODE", [("U1X", Decimal("9.75"), "唯一键增量中文"), ("U3", Decimal("3.33"), None)])
    results["unique_key"] = api(f"/api/jobs/{unique_job}").get("cdc_events")
    api(f"/api/jobs/{unique_job}/cancel", {})

    business_job = create_job(
        link_id,
        "QA-无主键-业务唯一键",
        names["business"],
        cdc_key_overrides={names["business"]: ["TENANT_ID", "ORDER_NO"]},
        cdc_allow_source_ddl=True,
    )
    wait_status(business_job, {"syncing"})
    with source.begin() as connection:
        connection.execute(text(f"UPDATE {names['business']} SET ORDER_NO='O1X', QTY=88.88, NOTE='业务键增量中文' WHERE TENANT_ID=1 AND ORDER_NO='O1'"))
        connection.execute(text(f"DELETE FROM {names['business']} WHERE TENANT_ID=1 AND ORDER_NO='O2'"))
        connection.execute(text(f"INSERT INTO {names['business']} VALUES (2,'O3',30.30,NULL)"))
    wait_rows(target, names["business"], "TENANT_ID,ORDER_NO,QTY,NOTE", "TENANT_ID,ORDER_NO", [(1, "O1X", Decimal("88.88"), "业务键增量中文"), (2, "O3", Decimal("30.30"), None)])
    results["business_key"] = api(f"/api/jobs/{business_job}").get("cdc_events")
    api(f"/api/jobs/{business_job}/cancel", {})

    all_job = create_job(
        link_id,
        "QA-无主键-ALL-COLUMNS",
        names["all"],
        cdc_no_key_policy="all_columns",
        cdc_allow_source_ddl=True,
    )
    wait_status(all_job, {"syncing"})
    with source.begin() as connection:
        connection.execute(text(f"UPDATE {names['all']} SET CODE='AX', QTY=7.77, NOTE='ALL 增量中文' WHERE CODE='A' AND QTY=1"))
        connection.execute(text(f"DELETE FROM {names['all']} WHERE CODE='B' AND QTY=2"))
        connection.execute(text(f"INSERT INTO {names['all']} VALUES ('C',3.00,NULL)"))
    wait_rows(target, names["all"], "CODE,QTY,NOTE", "CODE", [("AX", Decimal("7.77"), "ALL 增量中文"), ("C", Decimal("3.00"), None)])
    results["all_columns"] = api(f"/api/jobs/{all_job}").get("cdc_events")
    api(f"/api/jobs/{all_job}/cancel", {})

    rejection_cases = [
        ("no_key", names["none"], {}),
        ("duplicate_key", names["duplicate"], {"cdc_key_overrides": {names["duplicate"]: ["CODE"]}, "cdc_allow_source_ddl": True}),
        ("null_key", names["null"], {"cdc_key_overrides": {names["null"]: ["CODE"]}, "cdc_allow_source_ddl": True}),
    ]
    for label, table_name, options in rejection_cases:
        job_id = create_job(link_id, f"QA-无主键-安全拒绝-{label}", table_name, **options)
        failed = wait_status(job_id, {"failed"}, timeout=60)
        results[label] = failed.get("error")

    print(json.dumps({"ok": True, "results": results}, ensure_ascii=False, default=str))
    source.dispose()
    target.dispose()


if __name__ == "__main__":
    import sys

    main(sys.argv[1])
