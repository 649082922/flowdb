from __future__ import annotations

import re
import os
import hashlib
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Iterable

from sqlalchemy import Date, DateTime, MetaData, Table, inspect, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine

from .database import adapt_value, default_schema, make_engine
from .models import ConnectionConfig
from .store import JobStore, utc_now


_DML_CODES = {
    1: "insert",
    2: "delete",
    3: "update",
    10: "lob_write",
    11: "lob_trim",
    29: "lob_erase",
}


def make_logminer_engine(source: ConnectionConfig) -> Engine:
    """Use a dedicated CDB$ROOT account when the business link targets a PDB.

    A CDB/PDB full-load connection cannot call DBMS_LOGMNR.ADD_LOGFILE.  The
    dedicated credentials are server-side only and never returned to the web UI.
    Non-CDB installations may omit them and reuse the source connection.
    """
    username = os.environ.get("FLOWDB_LOGMINER_USERNAME", "").strip()
    password = os.environ.get("FLOWDB_LOGMINER_PASSWORD", "")
    service = os.environ.get("FLOWDB_LOGMINER_SERVICE", "").strip()
    if not (username and password and service):
        return make_engine(source)
    config = source.model_copy(
        update={"database": service, "username": username, "password": password, "schema_name": None}
    )
    return make_engine(config)


@dataclass(frozen=True)
class ChangeEvent:
    scn: int
    commit_scn: int
    xid: str
    operation: str
    owner: str
    table: str
    row_id: str | None
    sql_redo: str


def _object_parts(name: str, fallback_owner: str | None) -> tuple[str | None, str]:
    if "." in name:
        owner, table_name = name.split(".", 1)
        return owner.strip('"'), table_name.strip('"')
    return fallback_owner, name.strip('"')


def _decode_oracle_literal(raw: str) -> Any:
    value = raw.strip().rstrip(";")
    if value.upper() == "NULL":
        return None
    unistr_match = re.fullmatch(
        r"UNISTR\s*\(\s*'((?:''|[^'])*)'\s*\)", value, re.I | re.S
    )
    if unistr_match:
        escaped = unistr_match.group(1).replace("''", "'")
        decoded_units = re.sub(
            r"\\([0-9A-Fa-f]{4})",
            lambda match: chr(int(match.group(1), 16)),
            escaped,
        )
        # Oracle represents supplementary characters as UTF-16 surrogate
        # pairs (for example \D83D\DE42). Combine those pairs safely.
        return decoded_units.encode("utf-16-le", "surrogatepass").decode("utf-16-le")
    if (value.startswith("'") or value.upper().startswith("N'")) and value.endswith("'"):
        offset = 2 if value.upper().startswith("N'") else 1
        return value[offset:-1].replace("''", "'")
    hex_match = re.fullmatch(r"HEXTORAW\s*\(\s*'([0-9A-Fa-f]*)'\s*\)", value, re.I)
    if hex_match:
        return bytes.fromhex(hex_match.group(1))
    conversion = re.match(
        r"(?:TO_DATE|TO_TIMESTAMP(?:_TZ)?|TO_DSINTERVAL|TO_YMINTERVAL)\s*\(\s*'((?:''|[^'])*)'",
        value,
        re.I,
    )
    if conversion:
        return conversion.group(1).replace("''", "'")
    try:
        return Decimal(value)
    except Exception:
        return value


_ORACLE_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _coerce_predicate_value(value: Any, target_type: Any) -> Any:
    """Convert LogMiner text literals to the reflected target column type.

    Oracle commonly emits DATE values in SQL_REDO as locale-independent English
    literals such as ``02-MAR-26``.  MySQL/TDSQL rejects that string when it is
    bound to a DATE/DATETIME predicate, so normalize it before locating the old
    row.  Full-row writes do not need this because their values are fetched from
    Oracle through the driver as native datetime objects.
    """
    if value is None or not isinstance(value, str):
        return value
    if not isinstance(target_type, (Date, DateTime)):
        return value
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        match = re.fullmatch(
            r"(\d{1,2})-([A-Za-z]{3})-(\d{2}|\d{4})"
            r"(?:[ T](\d{1,2})[.:](\d{2})[.:](\d{2})(?:\.(\d{1,9}))?\s*(AM|PM)?)?"
            r"(?:\s*([+-]\d{2}:?\d{2}))?",
            raw,
            re.I,
        )
        if not match or match.group(2).upper() not in _ORACLE_MONTHS:
            return value
        year = int(match.group(3))
        if year < 100:
            year += 2000 if year <= 49 else 1900
        fraction = (match.group(7) or "")[:6].ljust(6, "0")
        hour = int(match.group(4) or 0)
        if (match.group(8) or "").upper() == "PM" and hour < 12:
            hour += 12
        elif (match.group(8) or "").upper() == "AM" and hour == 12:
            hour = 0
        parsed = datetime(
            year,
            _ORACLE_MONTHS[match.group(2).upper()],
            int(match.group(1)),
            hour,
            int(match.group(5) or 0),
            int(match.group(6) or 0),
            int(fraction or 0),
        )
    return parsed.date() if isinstance(target_type, Date) and not isinstance(target_type, DateTime) else parsed


def primary_key_predicate(sql_redo: str, primary_keys: Iterable[str]) -> dict[str, Any]:
    """Extract supplemental-logged PK equality predicates from SQL_REDO.

    LogMiner emits quoted identifiers and Oracle literals.  The scanner accepts
    the ordinary PK predicates produced for UPDATE/DELETE and intentionally
    rejects missing keys instead of risking a broad target-side modification.
    """
    predicates: dict[str, Any] = {}
    where_match = re.search(r"\bwhere\b(.*)$", sql_redo, re.I | re.S)
    predicate_sql = where_match.group(1) if where_match else sql_redo
    for key in primary_keys:
        pattern = re.compile(
            rf'(?:(?:"[^"]+"\.)?"{re.escape(key)}"|\b{re.escape(key)}\b)\s*=\s*'
            r"((?:N)?'(?:''|[^'])*'|NULL|UNISTR\s*\(\s*'(?:''|[^'])*'\s*\)|HEXTORAW\s*\(\s*'[0-9A-Fa-f]*'\s*\)|"
            r"(?:TO_DATE|TO_TIMESTAMP(?:_TZ)?|TO_DSINTERVAL|TO_YMINTERVAL)\s*\(.*?\)|"
            r"[-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)",
            re.I | re.S,
        )
        matches = pattern.findall(predicate_sql)
        if matches:
            predicates[key] = _decode_oracle_literal(matches[-1])
        elif re.search(
            rf'(?:(?:"[^"]+"\.)?"{re.escape(key)}"|\b{re.escape(key)}\b)\s+IS\s+NULL',
            predicate_sql,
            re.I,
        ):
            predicates[key] = None
    return predicates


def _split_sql_list(value: str) -> list[str]:
    """Split an Oracle expression list without breaking quoted/function values."""
    items: list[str] = []
    start = 0
    depth = 0
    quoted = False
    index = 0
    while index < len(value):
        char = value[index]
        if char == "'":
            if quoted and index + 1 < len(value) and value[index + 1] == "'":
                index += 2
                continue
            quoted = not quoted
        elif not quoted:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(0, depth - 1)
            elif char == "," and depth == 0:
                items.append(value[start:index].strip())
                start = index + 1
        index += 1
    items.append(value[start:].strip())
    return items


def primary_key_values(sql_redo: str, primary_keys: Iterable[str]) -> dict[str, Any]:
    """Read PK values from UPDATE/DELETE predicates or INSERT value lists."""
    keys = list(primary_keys)
    predicates = primary_key_predicate(sql_redo, keys)
    update_match = re.search(r"\bupdate\s+.+?\s+set\s+(.*?)\s+where\s+", sql_redo, re.I | re.S)
    if update_match:
        for assignment in _split_sql_list(update_match.group(1)):
            match = re.match(
                r'\s*(?:(?:"[^"]+"\.)?"([^"]+)"|([A-Za-z0-9_$#]+))\s*=\s*(.*)\s*$',
                assignment,
                re.S,
            )
            if not match:
                continue
            column = (match.group(1) or match.group(2)).upper()
            for key in keys:
                if key.upper() == column:
                    predicates[key] = _decode_oracle_literal(match.group(3))
    if len(predicates) == len(keys):
        return predicates
    insert_match = re.search(
        r"\binsert\s+into\s+.+?\((.*?)\)\s*values\s*\((.*)\)\s*;?\s*$",
        sql_redo,
        re.I | re.S,
    )
    if not insert_match:
        return predicates
    columns = [part.strip().strip('"') for part in _split_sql_list(insert_match.group(1))]
    values = _split_sql_list(insert_match.group(2))
    by_column = {
        column.upper(): _decode_oracle_literal(raw)
        for column, raw in zip(columns, values)
    }
    return {key: by_column[key.upper()] for key in keys if key.upper() in by_column}


def coalesce_logical_events(
    events: Iterable[ChangeEvent],
    selected: dict[tuple[str, str], str],
    primary_keys: dict[str, list[str]],
) -> list[ChangeEvent]:
    """Collapse Oracle's INSERT + LOB UPDATE into one logical row change.

    LogMiner represents an INSERT containing out-of-line CLOB/BLOB data as an
    INSERT with EMPTY_LOB placeholders followed by an UPDATE in the same
    transaction.  Replaying both is harmless but double-counts one user row
    change and performs a redundant target write.  A later UPDATE for the same
    replication key is therefore folded into the preceding INSERT; its ROWID
    and SQL are retained so the final, complete source row can still be read.
    """
    logical: list[ChangeEvent] = []
    inserted: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}

    def identity(source_name: str, keys: list[str], values: dict[str, Any]):
        if len(values) != len(keys):
            return None
        return (
            source_name.upper(),
            tuple((key.upper(), repr(values[key])) for key in keys),
        )

    for event in events:
        source_name = selected.get((event.owner.upper(), event.table.upper()))
        keys = primary_keys.get(source_name or "", [])
        current_identity = (
            identity(source_name, keys, primary_key_values(event.sql_redo, keys))
            if source_name and keys and event.operation in {"insert", "update"}
            else None
        )
        old_identity = (
            identity(source_name, keys, primary_key_predicate(event.sql_redo, keys))
            if source_name and keys and event.operation == "update"
            else None
        )
        prior_index = inserted.get(old_identity) if old_identity else None
        if prior_index is None and current_identity:
            prior_index = inserted.get(current_identity)
        if event.operation == "update" and prior_index is not None:
            prior = logical[prior_index]
            logical[prior_index] = ChangeEvent(
                scn=prior.scn,
                commit_scn=event.commit_scn,
                xid=event.xid,
                operation="insert",
                owner=event.owner,
                table=event.table,
                row_id=event.row_id or prior.row_id,
                sql_redo=event.sql_redo,
            )
            if current_identity:
                inserted[current_identity] = prior_index
            continue
        logical.append(event)
        if event.operation == "insert" and current_identity:
            inserted[current_identity] = len(logical) - 1
    return logical


def choose_replication_key(
    columns: list[dict[str, Any]],
    primary_key: Iterable[str],
    unique_constraints: Iterable[dict[str, Any]],
    unique_indexes: Iterable[dict[str, Any]],
    override: Iterable[str] | None = None,
    allow_all_columns: bool = False,
) -> tuple[str, list[str]]:
    """Choose the safest logical row identity available for CDC."""
    by_upper = {str(column["name"]).upper(): column for column in columns}

    def canonical(names: Iterable[str]) -> list[str]:
        result: list[str] = []
        for name in names:
            column = by_upper.get(str(name).upper())
            if column is None:
                return []
            result.append(str(column["name"]))
        return result

    if override:
        keys = canonical(override)
        if not keys:
            raise RuntimeError("用户指定业务键包含不存在的字段")
        return "business_key", keys
    keys = canonical(primary_key)
    if keys:
        return "primary_key", keys
    candidates: list[list[str]] = []
    for item in [*unique_constraints, *unique_indexes]:
        names = item.get("column_names") or item.get("constrained_columns") or []
        candidate = canonical(names)
        if candidate and all(not bool(by_upper[name.upper()].get("nullable", True)) for name in candidate):
            candidates.append(candidate)
    if candidates:
        candidates.sort(key=lambda item: (len(item), [name.upper() for name in item]))
        return "unique_key", candidates[0]
    if allow_all_columns:
        excluded = ("BLOB", "CLOB", "NCLOB", "BFILE", "LONG", "XMLTYPE", "OBJECT")
        scalar = [
            str(column["name"])
            for column in columns
            if not any(token in str(column.get("type", "")).upper() for token in excluded)
        ]
        if scalar:
            return "all_columns", scalar
    return "none", []


class OracleLogMiner:
    """Finite-SCN LogMiner reader compatible with Oracle 19c and newer.

    Oracle removed CONTINUOUS_MINE in 19c.  Each poll therefore registers the
    archived/online redo files covering a bounded SCN window, mines committed
    transactions, ends the session, and advances a durable checkpoint.
    """

    def __init__(self, engine: Engine):
        if engine.dialect.name != "oracle":
            raise ValueError("LogMiner 增量采集只支持 Oracle 源端")
        self.engine = engine

    def current_scn(self) -> int:
        with self.engine.connect() as connection:
            return int(connection.execute(text("SELECT current_scn FROM v$database")).scalar_one())

    def current_and_safe_end_scn(self) -> tuple[int, int]:
        """Read the high-water mark and oldest transaction in one snapshot.

        Reading them in separate statements leaves a race: a transaction can
        begin after CURRENT_SCN is read but before V$TRANSACTION is inspected,
        allowing the checkpoint to advance beyond its first DML. A single
        statement makes a transaction either visible (and therefore protected)
        or newer than the returned current SCN.
        """
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT d.current_scn, "
                    "(SELECT MIN(t.start_scn) FROM v$transaction t) AS oldest_start_scn "
                    "FROM v$database d"
                )
            ).mappings().one()
        current = int(row["current_scn"])
        oldest = row.get("oldest_start_scn")
        safe = current if oldest is None else max(0, min(current, int(oldest) - 1))
        return current, safe

    def capture_start_scn(self) -> int:
        """Include transactions already open when the full baseline starts."""
        with self.engine.connect() as connection:
            current = int(connection.execute(text("SELECT current_scn FROM v$database")).scalar_one())
            oldest = connection.execute(text("SELECT MIN(start_scn) FROM v$transaction")).scalar()
        return max(1, min(current, int(oldest)) - 1) if oldest is not None else current

    def safe_end_scn(self, current_scn: int) -> int:
        """Never checkpoint past the oldest open Oracle transaction.

        Finite LogMiner windows plus COMMITTED_DATA_ONLY would otherwise lose a
        long transaction whose row changes precede a checkpoint but whose COMMIT
        appears in a later window.
        """
        with self.engine.connect() as connection:
            oldest = connection.execute(text("SELECT MIN(start_scn) FROM v$transaction")).scalar()
        if oldest is None:
            return current_scn
        return max(0, min(current_scn, int(oldest) - 1))

    def capabilities(self) -> dict[str, Any]:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT log_mode, supplemental_log_data_min, supplemental_log_data_pk, "
                    "supplemental_log_data_all "
                    "FROM v$database"
                )
            ).mappings().one()
            connection.execute(text("SELECT COUNT(*) FROM v$archived_log")).scalar_one()
            connection.execute(text("SELECT COUNT(*) FROM v$logfile")).scalar_one()
            connection.execute(text("SELECT MIN(start_scn) FROM v$transaction")).scalar()
            container = connection.execute(
                text(
                    "SELECT SYS_CONTEXT('USERENV','CON_NAME') AS con_name, cdb "
                    "FROM v$database"
                )
            ).mappings().one()
        cdb_root_ready = (
            str(container["cdb"]).upper() != "YES"
            or str(container["con_name"]).upper() == "CDB$ROOT"
        )
        return {
            "log_mode": str(row["log_mode"]),
            "supplemental_log_data_min": str(row["supplemental_log_data_min"]),
            "supplemental_log_data_pk": str(row["supplemental_log_data_pk"]),
            "supplemental_log_data_all": str(row["supplemental_log_data_all"]),
            "container": str(container["con_name"]),
            "cdb_root_ready": cdb_root_ready,
            "ready": str(row["log_mode"]).upper() == "ARCHIVELOG"
            and str(row["supplemental_log_data_min"]).upper() in {"YES", "IMPLICIT"}
            and cdb_root_ready,
        }

    @staticmethod
    def _end_quietly(connection) -> None:
        try:
            connection.exec_driver_sql(
                "BEGIN DBMS_LOGMNR.END_LOGMNR; EXCEPTION WHEN OTHERS THEN NULL; END;"
            )
        except Exception:
            pass

    def _redo_files(self, connection, start_scn: int, end_scn: int) -> list[str]:
        rows = connection.execute(
            text(
                "SELECT name, first_change#, next_change# FROM v$archived_log "
                "WHERE name IS NOT NULL AND archived='YES' AND deleted='NO' "
                "AND next_change# > :start_scn AND first_change# <= :end_scn "
                "UNION ALL "
                "SELECT lf.member AS name, l.first_change#, "
                "CASE WHEN l.next_change# = 0 THEN :end_scn + 1 ELSE l.next_change# END "
                "FROM v$log l JOIN v$logfile lf ON lf.group# = l.group# "
                "WHERE l.first_change# <= :end_scn "
                "AND (l.next_change# = 0 OR l.next_change# > :start_scn)"
            ),
            {"start_scn": int(start_scn), "end_scn": int(end_scn)},
        ).mappings()
        return list(dict.fromkeys(str(row["name"]) for row in rows if row.get("name")))

    def validate_start_scn(self, start_scn: int) -> dict[str, int]:
        """Fail before table/key inspection when a checkpoint is no longer mineable."""
        with self.engine.connect() as connection:
            current_scn = int(
                connection.execute(text("SELECT current_scn FROM v$database")).scalar_one()
            )
            if int(start_scn) > current_scn:
                raise RuntimeError(
                    f"增量起始 SCN {start_scn} 大于 Oracle 当前 SCN {current_scn}"
                )
            files = self._redo_files(connection, int(start_scn), current_scn)
        if not files:
            raise RuntimeError(
                f"增量起始 SCN {start_scn} 对应的 redo/归档日志已不存在；"
                "无法安全继续，请重新执行全量基线"
            )
        return {"current_scn": current_scn, "redo_file_count": len(files)}

    def poll(self, start_scn: int, end_scn: int) -> list[ChangeEvent]:
        if end_scn < start_scn:
            return []
        with self.engine.connect() as connection:
            files = self._redo_files(connection, start_scn, end_scn)
            if not files:
                raise RuntimeError(
                    f"SCN {start_scn}-{end_scn} 没有可用 redo/归档日志；请检查归档保留策略"
                )
            self._end_quietly(connection)
            for index, file_name in enumerate(files):
                option = "DBMS_LOGMNR.NEW" if index == 0 else "DBMS_LOGMNR.ADDFILE"
                connection.exec_driver_sql(
                    f"BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :file_name, OPTIONS => {option}); END;",
                    {"file_name": file_name},
                )
            connection.exec_driver_sql(
                "BEGIN DBMS_LOGMNR.START_LOGMNR("
                "STARTSCN => :start_scn, ENDSCN => :end_scn, "
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + "
                "DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.NO_SQL_DELIMITER); END;",
                {"start_scn": int(start_scn), "end_scn": int(end_scn)},
            )
            try:
                rows = connection.execute(
                    text(
                        "SELECT scn, NVL(commit_scn, scn) AS commit_scn, xid, operation_code, "
                        "seg_owner, table_name, row_id, sql_redo, csf "
                        "FROM v$logmnr_contents "
                        "WHERE scn BETWEEN :start_scn AND :end_scn "
                        "AND operation_code IN (1,2,3,5,10,11,29) "
                        "ORDER BY NVL(commit_scn, scn), scn, rs_id, ssn"
                    ),
                    {"start_scn": int(start_scn), "end_scn": int(end_scn)},
                ).mappings()
                raw_rows = [dict(row) for row in rows]
            finally:
                self._end_quietly(connection)

        events: list[ChangeEvent] = []
        pending_sql = ""
        pending_row: dict[str, Any] | None = None
        for row in raw_rows:
            if pending_row is None:
                pending_row = row
                pending_sql = str(row.get("sql_redo") or "")
            else:
                pending_sql += str(row.get("sql_redo") or "")
            if int(row.get("csf") or 0) == 1:
                continue
            code = int((pending_row or {}).get("operation_code") or 0)
            operation = _DML_CODES.get(code, "unsupported")
            events.append(
                ChangeEvent(
                    scn=int((pending_row or {}).get("scn") or 0),
                    commit_scn=int((pending_row or {}).get("commit_scn") or 0),
                    xid=str((pending_row or {}).get("xid") or ""),
                    operation=operation,
                    owner=str((pending_row or {}).get("seg_owner") or ""),
                    table=str((pending_row or {}).get("table_name") or ""),
                    row_id=str((pending_row or {}).get("row_id") or "") or None,
                    sql_redo=pending_sql,
                )
            )
            pending_row = None
            pending_sql = ""
        return events


class IncrementalReplicator:
    def __init__(
        self,
        store: JobStore,
        job_id: str,
        payload: dict[str, Any],
        source_engine: Engine,
        target_engine: Engine,
        stop_event: threading.Event,
        logminer_engine: Engine | None = None,
    ):
        self.store = store
        self.job_id = job_id
        self.payload = payload
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.stop_event = stop_event
        self.source_config = ConnectionConfig.model_validate(payload["source"])
        self.target_config = ConnectionConfig.model_validate(payload["target"])
        self.source_schema = default_schema(self.source_config)
        self.target_schema = default_schema(self.target_config)
        self.miner = OracleLogMiner(logminer_engine or source_engine)
        self.poll_seconds = max(1.0, min(float(payload.get("cdc_poll_seconds", 3)), 60.0))
        self.window_scn = max(1000, min(int(payload.get("cdc_window_scn", 100000)), 1000000))
        self.selected: dict[tuple[str, str], str] = {}
        for source_name in payload.get("tables", []):
            if payload.get("object_types", {}).get(source_name) == "view":
                continue
            owner, table_name = _object_parts(source_name, self.source_schema)
            self.selected[((owner or "").upper(), table_name.upper())] = source_name
        self.target_names = payload.get("target_object_names", {})
        self._target_tables: dict[str, Table] = {}
        self._source_primary_keys: dict[str, list[str]] = {}
        self._key_strategies: dict[str, str] = {}

    def _override_for(self, source_name: str) -> list[str] | None:
        overrides = self.payload.get("cdc_key_overrides", {}) or {}
        owner, table_name = _object_parts(source_name, self.source_schema)
        aliases = {source_name.upper(), table_name.upper()}
        if owner:
            aliases.add(f"{owner}.{table_name}".upper())
        for configured_name, columns in overrides.items():
            if configured_name.upper() in aliases:
                return list(columns)
        return None

    def _validate_key_values(
        self, source_name: str, keys: list[str], *, allow_null: bool
    ) -> None:
        owner, table_name = _object_parts(source_name, self.source_schema)
        prep = self.source_engine.dialect.identifier_preparer
        qualified = (
            f"{prep.quote_schema(owner)}.{prep.quote(table_name)}" if owner else prep.quote(table_name)
        )
        quoted = [prep.quote(key) for key in keys]
        with self.source_engine.connect() as connection:
            if not allow_null:
                null_filter = " OR ".join(f"{column} IS NULL" for column in quoted)
                has_null = connection.execute(
                    text(f"SELECT 1 FROM {qualified} WHERE ({null_filter}) AND ROWNUM = 1")
                ).first()
                if has_null:
                    raise RuntimeError(f"{source_name} 的业务键包含 NULL：{', '.join(keys)}")
            grouped = ", ".join(quoted)
            duplicate = connection.execute(
                text(
                    f"SELECT 1 FROM (SELECT {grouped} FROM {qualified} "
                    f"GROUP BY {grouped} HAVING COUNT(*) > 1) WHERE ROWNUM = 1"
                )
            ).first()
        if duplicate:
            raise RuntimeError(f"{source_name} 的增量定位键存在重复值：{', '.join(keys)}")

    def _supplemental_groups(self, owner: str | None, table_name: str) -> list[dict[str, Any]]:
        with self.source_engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT g.log_group_name, g.log_group_type, g.always, c.column_name "
                    "FROM all_log_groups g LEFT JOIN all_log_group_columns c "
                    "ON c.owner=g.owner AND c.table_name=g.table_name "
                    "AND c.log_group_name=g.log_group_name "
                    "WHERE g.owner=:owner AND g.table_name=:table_name"
                ),
                {"owner": (owner or self.source_schema or "").upper(), "table_name": table_name.upper()},
            ).mappings()
            grouped: dict[str, dict[str, Any]] = {}
            for row in rows:
                item = grouped.setdefault(
                    str(row["log_group_name"]),
                    {
                        "type": str(row.get("log_group_type") or ""),
                        "always": str(row.get("always") or ""),
                        "columns": set(),
                    },
                )
                if row.get("column_name"):
                    item["columns"].add(str(row["column_name"]).upper())
        return list(grouped.values())

    def _ensure_supplemental_logging(
        self,
        source_name: str,
        strategy: str,
        keys: list[str],
        caps: dict[str, Any],
    ) -> None:
        if strategy == "primary_key" and caps["supplemental_log_data_pk"].upper() == "YES":
            return
        if strategy == "unique_key" and caps["supplemental_log_data_pk"].upper() == "YES":
            return
        if caps["supplemental_log_data_all"].upper() == "YES":
            return
        owner, table_name = _object_parts(source_name, self.source_schema)
        groups = self._supplemental_groups(owner, table_name)
        key_set = {key.upper() for key in keys}
        if strategy == "all_columns":
            ready = any("ALL COLUMN" in group["type"].upper() for group in groups)
        else:
            ready = any(
                group["always"].upper() == "ALWAYS" and key_set.issubset(group["columns"])
                for group in groups
            )
        if ready:
            return
        if not self.payload.get("cdc_allow_source_ddl", False):
            raise RuntimeError(
                f"{source_name} 缺少所需表级补充日志；请勾选允许创建源端补充日志后重试"
            )
        prep = self.source_engine.dialect.identifier_preparer
        qualified = (
            f"{prep.quote_schema(owner)}.{prep.quote(table_name)}" if owner else prep.quote(table_name)
        )
        if strategy == "all_columns":
            ddl = f"ALTER TABLE {qualified} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
        else:
            digest = hashlib.sha1(f"{owner}.{table_name}:{','.join(keys)}".encode()).hexdigest()[:16].upper()
            group_name = prep.quote(f"FDBC_{digest}")
            columns = ", ".join(prep.quote(key) for key in keys)
            ddl = f"ALTER TABLE {qualified} ADD SUPPLEMENTAL LOG GROUP {group_name} ({columns}) ALWAYS"
        with self.source_engine.begin() as connection:
            connection.exec_driver_sql(ddl)
        self.store.append_log(self.job_id, "WARN", f"[{source_name}] 已按用户授权创建源端补充日志")

    def preflight(self, start_scn: int | None = None) -> dict[str, Any]:
        caps = self.miner.capabilities()
        if not caps["ready"]:
            raise RuntimeError(
                "Oracle LogMiner 未就绪：必须启用 ARCHIVELOG 和最小补充日志；"
                f"当前 log_mode={caps['log_mode']}，supplemental_log_data_min={caps['supplemental_log_data_min']}"
            )
        if start_scn is not None:
            availability = self.miner.validate_start_scn(start_scn)
            self.store.append_log(
                self.job_id,
                "INFO",
                "LogMiner SCN 归档检查通过："
                f"起始 SCN={start_scn}，当前 SCN={availability['current_scn']}，"
                f"可用 redo/归档文件={availability['redo_file_count']} 个",
            )
        missing_key: list[str] = []
        inspector = inspect(self.source_engine)
        for source_name in self.selected.values():
            owner, table_name = _object_parts(source_name, self.source_schema)
            columns = list(inspector.get_columns(table_name, schema=owner))
            primary = list((inspector.get_pk_constraint(table_name, schema=owner) or {}).get("constrained_columns") or [])
            unique_constraints = list(inspector.get_unique_constraints(table_name, schema=owner) or [])
            unique_indexes = [
                item for item in (inspector.get_indexes(table_name, schema=owner) or []) if item.get("unique")
            ]
            strategy, keys = choose_replication_key(
                columns,
                primary,
                unique_constraints,
                unique_indexes,
                override=self._override_for(source_name),
                allow_all_columns=self.payload.get("cdc_no_key_policy") == "all_columns",
            )
            if strategy == "none":
                missing_key.append(source_name)
                continue
            if strategy in {"business_key", "all_columns"}:
                self._validate_key_values(source_name, keys, allow_null=strategy == "all_columns")
            self._ensure_supplemental_logging(source_name, strategy, keys, caps)
            self._source_primary_keys[source_name] = keys
            self._key_strategies[source_name] = strategy
            labels = {
                "primary_key": "主键",
                "unique_key": "非空唯一键",
                "business_key": "用户业务键",
                "all_columns": "ALL COLUMNS 风险键",
            }
            self.store.append_log(
                self.job_id,
                "WARN" if strategy == "all_columns" else "INFO",
                f"[{source_name}] 增量定位采用{labels[strategy]}：{', '.join(keys)}",
            )
        if missing_key:
            raise RuntimeError(
                "以下表没有主键、非空唯一键或已配置业务键，已安全阻止增量同步："
                + "、".join(missing_key[:30])
            )
        return caps

    def _target_table(self, source_name: str) -> Table:
        cached = self._target_tables.get(source_name)
        if cached is not None:
            return cached
        target_name = self.target_names.get(source_name, _object_parts(source_name, None)[1])
        table = Table(target_name, MetaData(), autoload_with=self.target_engine, schema=self.target_schema)
        self._target_tables[source_name] = table
        return table

    def _current_source_row(self, event: ChangeEvent, source_name: str) -> dict[str, Any] | None:
        owner, table_name = _object_parts(source_name, self.source_schema)
        prep = self.source_engine.dialect.identifier_preparer
        qualified = (
            f"{prep.quote_schema(owner)}.{prep.quote(table_name)}" if owner else prep.quote(table_name)
        )
        with self.source_engine.connect() as connection:
            row = None
            if event.row_id and set(event.row_id.upper()) != {"A"}:
                try:
                    row = connection.execute(
                        text(f"SELECT * FROM {qualified} WHERE ROWID = CHARTOROWID(:flowdb_rowid)"),
                        {"flowdb_rowid": event.row_id},
                    ).mappings().first()
                except Exception:
                    # CDB LogMiner may expose a ROWID that is not valid inside
                    # the PDB. Supplemental-logged PK values are the durable
                    # cross-container lookup key.
                    row = None
            if row is None:
                keys = self._source_primary_keys[source_name]
                key_values = primary_key_values(event.sql_redo, keys)
                if len(key_values) != len(keys):
                    return None
                clauses = []
                parameters: dict[str, Any] = {}
                for index, key in enumerate(keys):
                    bind = f"flowdb_pk_{index}"
                    if key_values[key] is None:
                        clauses.append(f"{prep.quote(key)} IS NULL")
                    else:
                        clauses.append(f"{prep.quote(key)} = :{bind}")
                        parameters[bind] = key_values[key]
                matches = connection.execute(
                    text(f"SELECT * FROM {qualified} WHERE {' AND '.join(clauses)}"),
                    parameters,
                ).mappings().fetchmany(2)
                if len(matches) > 1:
                    raise RuntimeError(
                        f"{source_name} 的增量定位键在源端匹配到多行，已停止以避免复制错误"
                    )
                row = matches[0] if matches else None
        return dict(row) if row else None

    @staticmethod
    def _case_value(row: dict[str, Any], name: str) -> Any:
        for key, value in row.items():
            if str(key).upper() == name.upper():
                return value
        raise KeyError(name)

    def _delete_old_key(self, connection, source_name: str, sql_redo: str) -> bool:
        target_table = self._target_table(source_name)
        source_keys = self._source_primary_keys[source_name]
        old_values = primary_key_predicate(sql_redo, source_keys)
        if len(old_values) != len(source_keys):
            return False
        target_by_upper = {column.name.upper(): column for column in target_table.columns}
        clauses = []
        for key in source_keys:
            column = target_by_upper.get(key.upper())
            if column is None:
                raise RuntimeError(f"目标表 {target_table.name} 缺少增量定位字段 {key}")
            clauses.append(column == _coerce_predicate_value(old_values[key], column.type))
        statement = target_table.delete()
        for clause in clauses:
            statement = statement.where(clause)
        result = connection.execute(statement)
        if result.rowcount is not None and result.rowcount > 1:
            raise RuntimeError(
                f"{source_name} 的定位键匹配到 {result.rowcount} 行，已停止以避免批量误删"
            )
        return True

    def _upsert_current_row(self, connection, source_name: str, event: ChangeEvent) -> bool:
        source_row = self._current_source_row(event, source_name)
        if source_row is None:
            return False
        target_table = self._target_table(source_name)
        payload: dict[str, Any] = {}
        for column in target_table.columns:
            if column.name == "__flowdb_row_id":
                continue
            try:
                value = self._case_value(source_row, column.name)
            except KeyError:
                continue
            payload[column.name] = adapt_value(value, column.type)
        if not payload:
            raise RuntimeError(f"无法构造 {source_name} 的增量行数据")
        # ALL COLUMNS/business-key tables may not have a physical unique key on
        # the target. Oracle can emit one logical UPDATE as scalar + LOB/BLOB
        # records; if an old-value predicate misses because of representation
        # differences, a plain INSERT would duplicate the row. Before replaying
        # an UPDATE, remove the already-present current version using native
        # values freshly read from Oracle. More than one match remains a hard
        # safety failure.
        if event.operation == "update" and self._key_strategies.get(source_name) in {
            "business_key",
            "all_columns",
        }:
            target_by_upper = {column.name.upper(): column for column in target_table.columns}
            current_delete = target_table.delete()
            for key in self._source_primary_keys[source_name]:
                column = target_by_upper.get(key.upper())
                if column is None:
                    raise RuntimeError(f"目标表 {target_table.name} 缺少增量定位字段 {key}")
                value = self._case_value(source_row, key)
                current_delete = current_delete.where(column == adapt_value(value, column.type))
            deleted = connection.execute(current_delete)
            if deleted.rowcount is not None and deleted.rowcount > 1:
                raise RuntimeError(
                    f"{source_name} 的当前定位键匹配到 {deleted.rowcount} 行，已停止以避免批量误删"
                )
        statement = mysql_insert(target_table).values(**payload)
        update_values = {
            column.name: statement.inserted[column.name]
            for column in target_table.columns
            if column.name in payload and not column.primary_key
        }
        if update_values:
            statement = statement.on_duplicate_key_update(**update_values)
        connection.execute(statement)
        return True

    def _event_key_summary(self, source_name: str, event: ChangeEvent) -> str:
        keys = self._source_primary_keys[source_name]
        old_values = primary_key_predicate(event.sql_redo, keys)
        new_values = primary_key_values(event.sql_redo, keys)

        def render(values: dict[str, Any]) -> str:
            parts = []
            for key in keys:
                if key not in values:
                    continue
                value = repr(values[key])
                parts.append(f"{key}={value[:80]}{'…' if len(value) > 80 else ''}")
            return ", ".join(parts) or "定位键未显示"

        if event.operation == "update" and old_values != new_values:
            return f"旧键[{render(old_values)}] → 新键[{render(new_values)}]"
        return render(old_values if event.operation == "delete" else new_values)

    def _apply_transaction(self, events: list[ChangeEvent]) -> tuple[int, dict[str, int]]:
        applied = 0
        breakdown = {"insert": 0, "update": 0, "delete": 0}
        applied_logs: list[tuple[ChangeEvent, str, str]] = []
        with self.target_engine.begin() as target_connection:
            for event in events:
                source_name = self.selected.get((event.owner.upper(), event.table.upper()))
                if source_name is None:
                    continue
                if event.operation == "delete":
                    if not self._delete_old_key(target_connection, source_name, event.sql_redo):
                        raise RuntimeError(
                            f"{source_name} DELETE 日志缺少完整定位键；请检查补充日志配置"
                        )
                    applied += 1
                    breakdown["delete"] += 1
                    applied_logs.append((event, source_name, self._event_key_summary(source_name, event)))
                elif event.operation in {"insert", "update"}:
                    if event.operation == "update":
                        self._delete_old_key(target_connection, source_name, event.sql_redo)
                    # Re-reading by ROWID transfers complete LOB/date/NUMBER values
                    # instead of executing Oracle-specific SQL_REDO on TDSQL.
                    if self._upsert_current_row(target_connection, source_name, event):
                        applied += 1
                        breakdown[event.operation] += 1
                        applied_logs.append((event, source_name, self._event_key_summary(source_name, event)))
                elif event.operation in {"lob_write", "lob_trim", "lob_erase"}:
                    # Oracle emits separate LOB fragments after the owning row DML.
                    # The INSERT/UPDATE branch re-reads the complete row by ROWID, so
                    # replaying these Oracle-specific fragments would be both
                    # redundant and invalid on MySQL/TDSQL.
                    continue
                else:
                    raise RuntimeError(
                        f"检测到暂不支持的 Oracle 日志操作，SCN={event.scn}，对象={event.owner}.{event.table}"
                    )
        operation_labels = {"insert": "INSERT", "update": "UPDATE", "delete": "DELETE"}
        for event, logged_source, key_summary in applied_logs:
            self.store.append_log(
                self.job_id,
                "INFO",
                f"[增量][SCN {event.scn}][{operation_labels[event.operation]}] "
                f"[{logged_source}] {key_summary} · 已写入目标端",
            )
        return applied, breakdown

    def run(self, start_scn: int) -> None:
        checkpoint = int(self.store.get(self.job_id).get("checkpoint_scn") or start_scn)
        transactions_total = int(self.store.get(self.job_id).get("cdc_transactions") or 0)
        events_total = int(self.store.get(self.job_id).get("cdc_events") or 0)
        inserts_total = int(self.store.get(self.job_id).get("cdc_inserts") or 0)
        updates_total = int(self.store.get(self.job_id).get("cdc_updates") or 0)
        deletes_total = int(self.store.get(self.job_id).get("cdc_deletes") or 0)
        # Never mine all the way to a high-water mark observed in the same
        # iteration. A very short transaction can begin around the dynamic-view
        # snapshot and commit before the next V$TRANSACTION read. Advancing only
        # to the previously observed SCN guarantees that such a transaction's
        # commit remains in a later complete window.
        observed_high_water = checkpoint
        self.store.update(
            self.job_id,
            status="catching_up",
            sync_phase="catching_up",
            current_table="增量追平 · LogMiner",
            cdc_started_at=utc_now(),
            checkpoint_scn=checkpoint,
            finished_at=None,
        )
        if self.payload.get("sync_mode") == "incremental_only":
            self.store.append_log(self.job_id, "INFO", f"从检查点 SCN {checkpoint} 开始回放增量")
        else:
            self.store.append_log(self.job_id, "INFO", f"全量完成，开始从 SCN {checkpoint} 回放增量")
        while not self.stop_event.is_set() and not self.store.cancelled(self.job_id):
            current, transaction_safe = self.miner.current_and_safe_end_scn()
            safe_current = min(transaction_safe, observed_high_water)
            observed_high_water = current
            if checkpoint >= safe_current:
                self.store.update(
                    self.job_id,
                    status="syncing",
                    sync_phase="realtime",
                    current_table="实时同步 · 等待 Oracle 变更",
                    source_current_scn=current,
                    cdc_lag=0,
                )
                self.stop_event.wait(self.poll_seconds)
                continue
            if safe_current <= checkpoint:
                self.store.update(
                    self.job_id,
                    status="catching_up",
                    sync_phase="catching_up",
                    current_table="增量追平 · 等待 Oracle 长事务提交",
                    source_current_scn=current,
                    cdc_lag=max(safe_current - checkpoint, 0),
                )
                self.stop_event.wait(self.poll_seconds)
                continue
            end_scn = min(safe_current, checkpoint + self.window_scn)
            # LogMiner STARTSCN must reach the first change of a transaction,
            # not only its COMMIT. Re-read an overlap window and deduplicate by
            # durable (COMMIT_SCN, XID), otherwise a transaction whose Oracle
            # START_SCN predates our observed checkpoint can appear as a partial
            # tail (commonly only the last partition-table LOB records).
            overlap = max(self.window_scn, 100000)
            poll_start = max(int(start_scn) + 1, checkpoint - overlap + 1)
            events = self.miner.poll(poll_start, end_scn)
            groups: dict[tuple[int, str], list[ChangeEvent]] = defaultdict(list)
            for event in events:
                if (event.owner.upper(), event.table.upper()) in self.selected:
                    groups[(event.commit_scn, event.xid)].append(event)
            for (commit_scn, _xid), transaction_events in sorted(groups.items()):
                if self.stop_event.is_set() or self.store.cancelled(self.job_id):
                    return
                if self.store.cdc_transaction_applied(self.job_id, commit_scn, _xid):
                    continue
                transaction_events = coalesce_logical_events(
                    transaction_events,
                    self.selected,
                    self._source_primary_keys,
                )
                dml_count = sum(event.operation in {"insert", "update", "delete"} for event in transaction_events)
                self.store.append_log(
                    self.job_id,
                    "INFO",
                    f"[增量事务] 开始回放 XID={_xid or '-'}，COMMIT_SCN={commit_scn}，DML={dml_count}",
                )
                applied, breakdown = self._apply_transaction(transaction_events)
                self.store.record_cdc_transaction(self.job_id, commit_scn, _xid)
                transactions_total += 1
                events_total += applied
                inserts_total += breakdown["insert"]
                updates_total += breakdown["update"]
                deletes_total += breakdown["delete"]
                checkpoint = max(checkpoint, commit_scn)
                self.store.update(
                    self.job_id,
                    checkpoint_scn=checkpoint,
                    cdc_transactions=transactions_total,
                    cdc_events=events_total,
                    cdc_inserts=inserts_total,
                    cdc_updates=updates_total,
                    cdc_deletes=deletes_total,
                    cdc_last_event_at=utc_now(),
                    source_current_scn=current,
                    cdc_lag=max(safe_current - checkpoint, 0),
                )
                self.store.append_log(
                    self.job_id,
                    "INFO",
                    f"[增量事务] COMMIT_SCN={commit_scn} 已提交目标端，本事务应用 {applied} 条 DML",
                )
            # finish-sync marks the task completed before signalling the worker.
            # Do not let a late heartbeat overwrite that terminal state.
            if self.stop_event.is_set() or self.store.cancelled(self.job_id):
                break
            checkpoint = max(checkpoint, end_scn)
            reached_realtime = end_scn >= safe_current
            self.store.update(
                self.job_id,
                status="syncing" if reached_realtime else "catching_up",
                sync_phase="realtime" if reached_realtime else "catching_up",
                current_table=(
                    "实时同步 · 等待 Oracle 变更"
                    if reached_realtime
                    else "增量追平 · LogMiner"
                ),
                checkpoint_scn=checkpoint,
                source_current_scn=current,
                cdc_lag=max(safe_current - checkpoint, 0),
            )
        terminal = self.store.get(self.job_id)
        if terminal["status"] == "completed":
            self.store.append_log(self.job_id, "INFO", "实时同步进程已正常结束")
        else:
            self.store.append_log(self.job_id, "WARN", "增量同步已停止")
