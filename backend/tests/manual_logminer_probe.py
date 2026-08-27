"""Manual end-to-end LogMiner probe for the configured 196 test link.

Run only against an explicitly authorized test database.  The script creates and
drops CLX.FLOWDB_CDC_PROBE and prints the mined DML without exposing passwords.
"""

from sqlalchemy import text

from app.cdc import OracleLogMiner, make_logminer_engine
from app.database import make_engine
from app.models import ConnectionConfig
from app.store import build_store


def main(link_id: str) -> None:
    link = build_store().get_link_payload(link_id)
    source = ConnectionConfig.model_validate(link["source"])
    engine = make_engine(source)
    logminer_engine = make_logminer_engine(source)
    miner = OracleLogMiner(logminer_engine)
    with engine.begin() as connection:
        try:
            connection.execute(text("DROP TABLE FLOWDB_CDC_PROBE PURGE"))
        except Exception:
            pass
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE FLOWDB_CDC_PROBE ("
                "ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(100), NOTE CLOB)"
            )
        )
    start_scn = miner.capture_start_scn()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO FLOWDB_CDC_PROBE(ID,NAME,NOTE) "
                "VALUES (1,'首次写入','中文 CLOB 增量测试')"
            )
        )
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE FLOWDB_CDC_PROBE SET NAME='更新后',NOTE='LOB 已更新' WHERE ID=1")
        )
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM FLOWDB_CDC_PROBE WHERE ID=1"))
    # A PDB connection cannot force a CDB-wide log switch (ORA-65040).  The
    # probe can still mine current online redo; operators may force a switch
    # from CDB$ROOT when explicitly testing archived-log discovery.
    end_scn = miner.current_scn()
    events = [
        event
        for event in miner.poll(start_scn + 1, end_scn)
        if event.owner.upper() == "CLX" and event.table.upper() == "FLOWDB_CDC_PROBE"
    ]
    print(
        {
            "start_scn": start_scn,
            "end_scn": end_scn,
            "operations": [event.operation for event in events],
            "row_ids_present": [bool(event.row_id) for event in events],
            "sql_redo_present": [bool(event.sql_redo) for event in events],
        }
    )
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE FLOWDB_CDC_PROBE PURGE"))
    engine.dispose()
    logminer_engine.dispose()
    if not {"insert", "update", "delete"}.issubset({event.operation for event in events}):
        raise SystemExit("LogMiner probe did not capture all INSERT/UPDATE/DELETE operations")


if __name__ == "__main__":
    import sys

    main(sys.argv[1])
