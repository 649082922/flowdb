"""196-only comprehensive Oracle -> TDSQL full + continuous CDC test.

Matrix: ordinary/partitioned x PK/no-PK, 13 tables, eight committed DML rounds.
This is intentionally a manual E2E script because it creates Oracle objects and
writes to the configured TDSQL test database.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.request
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text

from app.database import make_engine
from app.models import ConnectionConfig
from app.store import build_store


API = "http://127.0.0.1:8000"
PREFIX = "FLOWDB_CDCF_0826"
ROWS = 100
ROUNDS = 8


def api(path: str, payload: dict | None = None):
    request = urllib.request.Request(
        API + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        method="POST" if payload is not None else "GET",
        headers={
            "content-type": "application/json",
            "x-flowdb-token": os.environ["FLOWDB_API_TOKEN"],
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def wait_realtime(job_id: str, timeout: int = 300):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        job = api(f"/api/jobs/{job_id}")
        if job["status"] == "failed":
            raise RuntimeError(job.get("error"))
        if job["status"] == "syncing" and int(job.get("cdc_lag") or 0) == 0:
            return job
        time.sleep(1)
    raise TimeoutError(f"任务未在 {timeout}s 内进入实时同步")


def canonical(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        result = format(value, "f")
        if "." in result:
            result = result.rstrip("0").rstrip(".")
        return result or "0"
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex().upper()
    return str(value)


def digest_rows(connection, table_name: str) -> str:
    rows = connection.execute(
        text(
            f"SELECT ID,CODE,REGION,AMOUNT,NOTE,NULLABLE_TEXT "
            f"FROM {table_name} ORDER BY ID,CODE"
        )
    ).fetchall()
    payload = json.dumps(
        [[canonical(value) for value in row] for row in rows],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def main(link_id: str):
    link = build_store().get_link_payload(link_id)
    source = make_engine(ConnectionConfig.model_validate(link["source"]))
    target = make_engine(ConnectionConfig.model_validate(link["target"]))

    ordinary_pk = ["O_PK_NUM", "O_PK_TXT"]
    ordinary_no_pk = ["O_NPK_UQ", "O_NPK_BK", "O_NPK_ALL"]
    partition_pk = ["P_RANGE_PK", "P_LIST_PK", "P_HASH_PK", "P_INTERVAL_PK"]
    partition_no_pk = ["P_RANGE_UQ", "P_LIST_BK", "P_HASH_ALL", "P_INTERVAL_UQ"]
    all_keys = ordinary_pk + ordinary_no_pk + partition_pk + partition_no_pk
    tables = {key: f"{PREFIX}_{key}" for key in all_keys}
    migration_keys = ordinary_pk + ordinary_no_pk + partition_pk
    migration_tables = {key: tables[key] for key in migration_keys}

    common = """(
        ID NUMBER(12) NOT NULL,
        CODE VARCHAR2(40) NOT NULL,
        REGION VARCHAR2(10) NOT NULL,
        EVENT_DATE DATE NOT NULL,
        TS TIMESTAMP(6),
        AMOUNT NUMBER(30,10),
        NOTE NVARCHAR2(300),
        BODY CLOB,
        BIN BLOB,
        NULLABLE_TEXT VARCHAR2(200)
    )"""
    ddl = {
        "O_PK_NUM": common.replace("\n    )", ",\n        CONSTRAINT CDCF_OPKN_PK PRIMARY KEY (ID)\n    )"),
        "O_PK_TXT": common.replace("\n    )", ",\n        CONSTRAINT CDCF_OPKT_PK PRIMARY KEY (CODE)\n    )"),
        "O_NPK_UQ": common.replace("\n    )", ",\n        CONSTRAINT CDCF_ONUQ_UQ UNIQUE (CODE)\n    )"),
        "O_NPK_BK": common,
        "O_NPK_ALL": common,
        "P_RANGE_PK": common.replace("\n    )", ",\n        CONSTRAINT CDCF_PRPK_PK PRIMARY KEY (ID)\n    )")
            + " PARTITION BY RANGE (EVENT_DATE) ("
            "PARTITION P_OLD VALUES LESS THAN (DATE '2026-01-01'),"
            "PARTITION P_2026 VALUES LESS THAN (DATE '2027-01-01'),"
            "PARTITION P_MAX VALUES LESS THAN (MAXVALUE)) ENABLE ROW MOVEMENT",
        "P_LIST_PK": common.replace("\n    )", ",\n        CONSTRAINT CDCF_PLPK_PK PRIMARY KEY (REGION,ID)\n    )")
            + " PARTITION BY LIST (REGION) ("
            "PARTITION P_N VALUES ('NORTH'), PARTITION P_S VALUES ('SOUTH'),"
            "PARTITION P_E VALUES ('EAST')) ENABLE ROW MOVEMENT",
        "P_HASH_PK": common.replace("\n    )", ",\n        CONSTRAINT CDCF_PHPK_PK PRIMARY KEY (ID)\n    )")
            + " PARTITION BY HASH (ID) PARTITIONS 4",
        "P_INTERVAL_PK": common.replace("\n    )", ",\n        CONSTRAINT CDCF_PIPK_PK PRIMARY KEY (ID)\n    )")
            + " PARTITION BY RANGE (EVENT_DATE) INTERVAL (NUMTOYMINTERVAL(1,'MONTH')) ("
            "PARTITION P0 VALUES LESS THAN (DATE '2026-01-01')) ENABLE ROW MOVEMENT",
        "P_RANGE_UQ": common.replace("\n    )", ",\n        CONSTRAINT CDCF_PRUQ_UQ UNIQUE (CODE)\n    )")
            + " PARTITION BY RANGE (EVENT_DATE) ("
            "PARTITION P_OLD VALUES LESS THAN (DATE '2026-01-01'),"
            "PARTITION P_2026 VALUES LESS THAN (DATE '2027-01-01'),"
            "PARTITION P_MAX VALUES LESS THAN (MAXVALUE)) ENABLE ROW MOVEMENT",
        "P_LIST_BK": common
            + " PARTITION BY LIST (REGION) ("
            "PARTITION P_N VALUES ('NORTH'), PARTITION P_S VALUES ('SOUTH'),"
            "PARTITION P_E VALUES ('EAST')) ENABLE ROW MOVEMENT",
        "P_HASH_ALL": common + " PARTITION BY HASH (ID) PARTITIONS 4",
        "P_INTERVAL_UQ": common.replace("\n    )", ",\n        CONSTRAINT CDCF_PIUQ_UQ UNIQUE (CODE)\n    )")
            + " PARTITION BY RANGE (EVENT_DATE) INTERVAL (NUMTOYMINTERVAL(1,'MONTH')) ("
            "PARTITION P0 VALUES LESS THAN (DATE '2026-01-01')) ENABLE ROW MOVEMENT",
    }

    with source.begin() as connection:
        for name in tables.values():
            connection.exec_driver_sql(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {name} PURGE'; "
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
            )
        for key, name in tables.items():
            connection.exec_driver_sql(f"CREATE TABLE {name} {ddl[key]}")

        insert_sql = "(ID,CODE,REGION,EVENT_DATE,TS,AMOUNT,NOTE,BODY,BIN,NULLABLE_TEXT) VALUES (:id,:code,:region,:event_date,:ts,:amount,:note,:body,:bin,:nullable_text)"
        rows = []
        for index in range(1, ROWS + 1):
            rows.append(
                {
                    "id": index,
                    "code": f"C{index:05d}",
                    "region": ("NORTH", "SOUTH", "EAST")[index % 3],
                    "event_date": datetime(2025 + (index % 3), (index % 12) + 1, (index % 27) + 1),
                    "ts": datetime(2026, 8, 26, 9, index % 60, index % 60, index * 997 % 1_000_000),
                    "amount": Decimal(index) / Decimal("1000.0000000000"),
                    "note": f"基线-{index}-中文🙂",
                    "body": (f"CLOB-{index}-中文🙂") * (1 + index % 40),
                    "bin": bytes((index + offset) % 256 for offset in range(16 + index % 128)),
                    "nullable_text": None if index % 4 == 0 else f"可空-{index}",
                }
            )
        for name in tables.values():
            connection.execute(text(f"INSERT INTO {name} {insert_sql}"), rows)

    object_types = {
        name: "partitioned_table" if key in partition_pk + partition_no_pk else "table"
        for key, name in migration_tables.items()
    }
    overrides = {
        tables["O_NPK_BK"]: ["REGION", "ID"],
    }
    # TDSQL requires a primary key for physical partition tables. Verify that
    # FlowDB rejects an Oracle no-PK partition table instead of silently
    # changing uniqueness semantics or flattening it without disclosure.
    rejected = api("/api/jobs", {
        "name": "QA-无主键分区表安全拦截",
        "link_id": link_id,
        "tables": [tables["P_LIST_BK"]],
        "object_types": {tables["P_LIST_BK"]: "partitioned_table"},
        "batch_size": 100,
        "table_concurrency": 1,
        "existing_table": "drop_and_create",
        "migration_content": "structure_and_data",
        "fail_policy": "stop_on_error",
        "migrate_sequences": False,
        "sync_mode": "full_and_incremental",
        "cdc_poll_seconds": 1,
        "cdc_no_key_policy": "all_columns",
        "cdc_allow_source_ddl": True,
    })
    rejected_deadline = time.monotonic() + 120
    while time.monotonic() < rejected_deadline:
        rejected = api(f"/api/jobs/{rejected['id']}")
        if rejected["status"] == "failed":
            break
        time.sleep(0.5)
    if rejected["status"] != "failed" or "TDSQL 禁止无主键表" not in (rejected.get("error") or ""):
        raise AssertionError({"partition_no_pk_guard": rejected})

    payload = {
        "name": "QA-CDC全面持续同步-主键无主键-普通分区",
        "link_id": link_id,
        "tables": list(migration_tables.values()),
        "object_types": object_types,
        "batch_size": 100,
        "table_concurrency": 4,
        "existing_table": "drop_and_create",
        "migration_content": "structure_and_data",
        "fail_policy": "stop_on_error",
        "migrate_sequences": False,
        "sync_mode": "full_and_incremental",
        "cdc_poll_seconds": 1,
        "cdc_window_scn": 50000,
        "cdc_key_overrides": overrides,
        "cdc_no_key_policy": "all_columns",
        "cdc_allow_source_ddl": True,
    }
    job = api("/api/jobs", payload)
    job_id = job["id"]
    realtime = wait_realtime(job_id)

    with target.connect() as connection:
        baseline = {
            key: int(connection.execute(text(f"SELECT COUNT(*) FROM {name.lower()}")).scalar_one())
            for key, name in migration_tables.items()
        }
    if any(value != ROWS for value in baseline.values()):
        raise AssertionError({"baseline": baseline})

    round_results = []
    for round_no in range(1, ROUNDS + 1):
        deleted_id = 10 + round_no
        inserted_id = 10000 + round_no
        marker = f"实时轮次-{round_no}-中文🙂"
        with source.begin() as connection:
            for key, name in migration_tables.items():
                connection.execute(
                    text(
                        f"UPDATE {name} SET AMOUNT=:amount,NOTE=:note,BODY=:body,"
                        "BIN=:bin,NULLABLE_TEXT=:nullable WHERE ID=1"
                    ),
                    {
                        "amount": Decimal(round_no) / Decimal("10000000000"),
                        "note": marker,
                        "body": marker * (100 + round_no),
                        "bin": bytes([round_no]) * (500 + round_no),
                        "nullable": None if round_no % 2 == 0 else f"非空-{round_no}",
                    },
                )
                connection.execute(text(f"DELETE FROM {name} WHERE ID=:id"), {"id": deleted_id})
                connection.execute(
                    text(f"INSERT INTO {name} {insert_sql}"),
                    {
                        "id": inserted_id,
                        "code": f"N{round_no:05d}",
                        "region": ("NORTH", "SOUTH", "EAST")[round_no % 3],
                        "event_date": datetime(2027, (round_no % 12) + 1, (round_no % 27) + 1),
                        "ts": datetime(2026, 8, 26, 10, round_no, round_no, round_no * 111111 % 1_000_000),
                        "amount": Decimal("-1234567890.1234567890") + round_no,
                        "note": f"新增轮次-{round_no}",
                        "body": f"新增 CLOB {round_no} 中文🙂" * 150,
                        "bin": bytes([255 - round_no]) * (700 + round_no),
                        "nullable_text": None,
                    },
                )

        started = time.monotonic()
        deadline = started + 90
        checks = {}
        while time.monotonic() < deadline:
            current = api(f"/api/jobs/{job_id}")
            if current["status"] == "failed":
                raise RuntimeError(current.get("error"))
            with target.connect() as connection:
                checks = {}
                for key, name in migration_tables.items():
                    target_name = name.lower()
                    count = int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name}")).scalar_one())
                    updated = int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name} WHERE ID=1 AND NOTE=:note"), {"note": marker}).scalar_one())
                    inserted = int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name} WHERE ID=:id"), {"id": inserted_id}).scalar_one())
                    deleted = int(connection.execute(text(f"SELECT COUNT(*) FROM {target_name} WHERE ID=:id"), {"id": deleted_id}).scalar_one())
                    checks[key] = (count, updated, inserted, deleted)
            if all(value == (ROWS, 1, 1, 0) for value in checks.values()):
                break
            time.sleep(0.5)
        else:
            raise AssertionError({"round": round_no, "checks": checks})
        round_results.append(
            {
                "round": round_no,
                "latency_ms": round((time.monotonic() - started) * 1000),
                "tables_ok": len(checks),
                "checkpoint_scn": api(f"/api/jobs/{job_id}").get("checkpoint_scn"),
            }
        )
        time.sleep(1.5)

    hashes = {}
    details = {}
    with source.connect() as source_connection, target.connect() as target_connection:
        for key, name in migration_tables.items():
            source_hash = digest_rows(source_connection, name)
            target_hash = digest_rows(target_connection, name.lower())
            hashes[key] = {"source": source_hash, "target": target_hash, "equal": source_hash == target_hash}
        for key in ("O_PK_NUM", "O_NPK_ALL", "P_RANGE_PK", "P_LIST_PK", "P_HASH_PK", "P_INTERVAL_PK"):
            name = tables[key].lower()
            details[key] = tuple(
                target_connection.execute(
                    text(
                        f"SELECT ID,CODE,REGION,AMOUNT,NOTE,CHAR_LENGTH(BODY),"
                        f"OCTET_LENGTH(BIN),NULLABLE_TEXT FROM {name} WHERE ID=1"
                    )
                ).one()
            )
    if not all(item["equal"] for item in hashes.values()):
        raise AssertionError({"hashes": hashes})

    before_finish = api(f"/api/jobs/{job_id}")
    expected_inserts = len(migration_tables) * ROUNDS
    expected_deletes = len(migration_tables) * ROUNDS
    if int(before_finish.get("cdc_inserts") or 0) != expected_inserts:
        raise AssertionError({"cdc_inserts": before_finish.get("cdc_inserts"), "expected": expected_inserts})
    if int(before_finish.get("cdc_deletes") or 0) != expected_deletes:
        raise AssertionError({"cdc_deletes": before_finish.get("cdc_deletes"), "expected": expected_deletes})
    breakdown_total = sum(
        int(before_finish.get(key) or 0)
        for key in ("cdc_inserts", "cdc_updates", "cdc_deletes")
    )
    if breakdown_total != int(before_finish.get("cdc_events") or 0):
        raise AssertionError({"breakdown_total": breakdown_total, "cdc_events": before_finish.get("cdc_events")})

    log_payload = api(f"/api/jobs/{job_id}/logs?after_seq=0")
    dml_scns = []
    for entry in log_payload["logs"]:
        match = re.search(r"\[增量\]\[SCN (\d+)\]\[(INSERT|UPDATE|DELETE)\]", entry["message"])
        if match:
            dml_scns.append(int(match.group(1)))
    if dml_scns != sorted(dml_scns):
        raise AssertionError("增量 DML 日志未按 SCN 顺序输出")
    if len(dml_scns) != breakdown_total:
        raise AssertionError({"dml_logs": len(dml_scns), "cdc_events": breakdown_total})

    finished = api(f"/api/jobs/{job_id}/finish-sync", {})
    time.sleep(2)
    finished = api(f"/api/jobs/{job_id}")
    if finished["status"] != "completed":
        raise AssertionError({"terminal": finished["status"]})

    print(
        json.dumps(
            {
                "ok": True,
                "job_id": job_id,
                "matrix": {
                    "ordinary_pk": len(ordinary_pk),
                    "ordinary_no_pk": len(ordinary_no_pk),
                    "partition_pk": len(partition_pk),
                    "partition_no_pk_guarded": len(partition_no_pk),
                    "migrated_tables": len(migration_tables),
                    "baseline_rows": sum(baseline.values()),
                },
                "partition_no_pk_guard": {
                    "job_id": rejected["id"],
                    "status": rejected["status"],
                    "reason_verified": "TDSQL 禁止无主键表" in (rejected.get("error") or ""),
                },
                "start_scn": realtime.get("start_scn"),
                "rounds": round_results,
                "hashes": hashes,
                "details": details,
                "cdc_events": before_finish.get("cdc_events"),
                "cdc_transactions": before_finish.get("cdc_transactions"),
                "cdc_inserts": before_finish.get("cdc_inserts"),
                "cdc_updates": before_finish.get("cdc_updates"),
                "cdc_deletes": before_finish.get("cdc_deletes"),
                "cumulative_processed": sum(baseline.values()) + breakdown_total,
                "inferred_target_rows": (
                    sum(baseline.values())
                    + int(before_finish.get("cdc_inserts") or 0)
                    - int(before_finish.get("cdc_deletes") or 0)
                ),
                "dml_log_count": len(dml_scns),
                "dml_log_scn_ordered": dml_scns == sorted(dml_scns),
                "checkpoint_scn": before_finish.get("checkpoint_scn"),
                "terminal_status": finished.get("status"),
            },
            ensure_ascii=False,
            default=str,
        )
    )


if __name__ == "__main__":
    import sys

    main(sys.argv[1])
