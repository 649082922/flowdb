import unittest
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime

from app.cdc import (
    ChangeEvent,
    _coerce_predicate_value,
    choose_replication_key,
    coalesce_logical_events,
    primary_key_predicate,
    primary_key_values,
)


class LogMinerSqlParsingTests(unittest.TestCase):
    def test_insert_followed_by_lob_update_is_one_logical_insert(self):
        events = [
            ChangeEvent(
                100, 110, "xid", "insert", "CLX", "MIXED", "AAAAAAAAAAAAAAAAAA",
                'insert into "CLX"."MIXED"("ID","PAYLOAD") values (\'255\',EMPTY_BLOB())',
            ),
            ChangeEvent(
                100, 110, "xid", "update", "CLX", "MIXED", "AAASoBAAMAAABSdAAA",
                'update "CLX"."MIXED" set "PAYLOAD"=HEXTORAW(\'CAFE\') where "ID"=\'255\'',
            ),
        ]
        logical = coalesce_logical_events(
            events,
            {("CLX", "MIXED"): "CLX.MIXED"},
            {"CLX.MIXED": ["ID"]},
        )
        self.assertEqual(len(logical), 1)
        self.assertEqual(logical[0].operation, "insert")
        self.assertEqual(logical[0].row_id, "AAASoBAAMAAABSdAAA")
        self.assertIn("HEXTORAW", logical[0].sql_redo)

    def test_extracts_number_and_unicode_primary_key(self):
        sql = (
            'update "CLX"."ORDERS" set "NAME" = \'新值\' '
            'where "ID" = 1001 and "TENANT_CODE" = \'华北\''
        )
        self.assertEqual(
            primary_key_predicate(sql, ["ID", "TENANT_CODE"]),
            {"ID": Decimal("1001"), "TENANT_CODE": "华北"},
        )

    def test_delete_supports_raw_and_null(self):
        sql = (
            'delete from "CLX"."RAW_KEYS" where '
            '"RAW_ID" = HEXTORAW(\'00FF10\') and "DELETED_AT" = NULL'
        )
        self.assertEqual(
            primary_key_predicate(sql, ["RAW_ID", "DELETED_AT"]),
            {"RAW_ID": bytes.fromhex("00FF10"), "DELETED_AT": None},
        )

    def test_all_columns_delete_supports_oracle_unistr(self):
        sql = (
            'delete from "CLX"."NO_PK" where "ID" = \'11\' '
            'and "NOTE" = UNISTR(\'\\57FA\\7EBF-11-\\4E2D\\6587\\D83D\\DE42\')'
        )
        self.assertEqual(
            primary_key_predicate(sql, ["ID", "NOTE"]),
            {"ID": "11", "NOTE": "基线-11-中文🙂"},
        )

    def test_missing_primary_key_is_not_guessed(self):
        self.assertEqual(
            primary_key_predicate(
                'delete from "CLX"."T" where "NAME" = \'same\'', ["ID"]
            ),
            {},
        )

    def test_insert_primary_key_values_support_functions_and_commas(self):
        sql = (
            'insert into "CLX"."T"("ID","TENANT","STAMP","NAME") values '
            "('10','华北',TO_TIMESTAMP('2026-08-25 10:00:00.123456', "
            "'YYYY-MM-DD HH24:MI:SS.FF6'),'逗号,仍属于值');"
        )
        self.assertEqual(
            primary_key_values(sql, ["ID", "TENANT"]),
            {"ID": "10", "TENANT": "华北"},
        )

    def test_update_uses_new_key_and_supports_is_null(self):
        sql = (
            'update "CLX"."T" set "CODE" = \'NEW\', "NAME" = \'值\' '
            'where "CODE" = \'OLD\' and "DELETED_AT" IS NULL;'
        )
        self.assertEqual(
            primary_key_predicate(sql, ["CODE", "DELETED_AT"]),
            {"CODE": "OLD", "DELETED_AT": None},
        )
        self.assertEqual(primary_key_values(sql, ["CODE"]), {"CODE": "NEW"})

    def test_old_predicate_ignores_new_value_in_set_clause(self):
        sql = (
            'update "CLX"."T" set "NULLABLE_TEXT" = \'new-value\' '
            'where "ID" = \'1\' and "NULLABLE_TEXT" IS NULL'
        )
        self.assertEqual(
            primary_key_predicate(sql, ["ID", "NULLABLE_TEXT"]),
            {"ID": "1", "NULLABLE_TEXT": None},
        )

    def test_oracle_date_predicate_is_coerced_for_tdsql(self):
        self.assertEqual(_coerce_predicate_value("02-MAR-26", Date()), date(2026, 3, 2))
        self.assertEqual(
            _coerce_predicate_value("25-AUG-2026 14:03:04.123456", DateTime()),
            datetime(2026, 8, 25, 14, 3, 4, 123456),
        )
        self.assertEqual(_coerce_predicate_value("普通文本", DateTime()), "普通文本")
        self.assertEqual(
            _coerce_predicate_value("26-AUG-26 09.01.01 AM", DateTime()),
            datetime(2026, 8, 26, 9, 1, 1),
        )

    def test_key_selection_prefers_pk_then_non_null_unique(self):
        columns = [
            {"name": "ID", "nullable": False, "type": "NUMBER"},
            {"name": "CODE", "nullable": False, "type": "VARCHAR2"},
            {"name": "NOTE", "nullable": True, "type": "CLOB"},
        ]
        self.assertEqual(
            choose_replication_key(columns, ["ID"], [], [], allow_all_columns=True),
            ("primary_key", ["ID"]),
        )
        self.assertEqual(
            choose_replication_key(
                columns, [], [{"column_names": ["CODE"]}], [], allow_all_columns=True
            ),
            ("unique_key", ["CODE"]),
        )

    def test_nullable_unique_is_rejected_and_all_columns_excludes_lob(self):
        columns = [
            {"name": "CODE", "nullable": True, "type": "VARCHAR2"},
            {"name": "NOTE", "nullable": True, "type": "CLOB"},
        ]
        self.assertEqual(
            choose_replication_key(columns, [], [], [{"unique": True, "column_names": ["CODE"]}]),
            ("none", []),
        )
        self.assertEqual(
            choose_replication_key(columns, [], [], [], allow_all_columns=True),
            ("all_columns", ["CODE"]),
        )


if __name__ == "__main__":
    unittest.main()
