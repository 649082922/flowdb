import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, func, select
from sqlalchemy import BigInteger, DateTime, LargeBinary, Numeric, Text
from sqlalchemy.dialects import mysql, oracle

from app.database import (
    _partition_keys_missing_from_primary_key,
    _render_partitioned_create_ddl,
    build_partition_clause,
    build_target_table_name_map,
    copy_batches,
    list_objects,
    portable_type,
    prepare_table,
    resolve_table_name_policy,
)
from app.validation import canonical


class PortableTypeTests(unittest.TestCase):
    def test_identifier_case_auto_follows_target_setting(self):
        self.assertEqual(resolve_table_name_policy("auto", 1), "lower")
        self.assertEqual(resolve_table_name_policy("auto", 0), "preserve")
        self.assertEqual(resolve_table_name_policy("auto", None), "preserve")

    def test_preserve_is_rejected_when_target_stores_lowercase(self):
        with self.assertRaisesRegex(ValueError, "lower_case_table_names=1"):
            resolve_table_name_policy("preserve", 1)

    def test_identifier_mapping_preserves_schema_and_detects_collision(self):
        self.assertEqual(
            build_target_table_name_map(["CLX.MixedName"], "lower", 1),
            {"CLX.MixedName": "CLX.mixedname"},
        )
        with self.assertRaisesRegex(ValueError, "目标对象名冲突"):
            build_target_table_name_map(["CaseTable", "CASETABLE"], "lower", 1)

    def test_partition_key_gap_is_detected_without_changing_source_pk(self):
        info = {
            "partitioning_type": "RANGE",
            "subpartitioning_type": "NONE",
            "partition_key_columns": ["BIZ_DATE"],
        }
        self.assertEqual(
            _partition_keys_missing_from_primary_key(info, ["ID"]),
            ["BIZ_DATE"],
        )
        self.assertEqual(
            _partition_keys_missing_from_primary_key(info, ["ID", "BIZ_DATE"]),
            [],
        )

    def test_oracle_partition_tables_are_classified_with_four_bulk_queries(self):
        normal_names = [f"CLX_NORMAL_{index:02d}" for index in range(1, 41)]
        partition_names = [
            "TST_PART_HASH",
            "TST_PART_INTERVAL",
            "TST_PART_LIST",
            "TST_PART_RANGE_COLS",
            "TST_PART_RANGE_DATE",
            "TST_PART_RANGE_HASH",
            "TST_PART_RANGE_INT",
            "TST_PART_RANGE_LIST",
        ]
        object_rows = [
            {"owner": "CLX", "object_name": name, "object_type": "table"}
            for name in normal_names
        ] + [
            {"owner": "CLX", "object_name": name, "object_type": "partitioned_table"}
            for name in partition_names
        ]

        class FakeResult(list):
            def mappings(self):
                return self

        class FakeConnection:
            calls = 0

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, statement, parameters):
                self.calls += 1
                self.assert_owners(parameters)
                sql = str(statement).lower()
                if "union all" in sql:
                    return FakeResult(object_rows)
                if "count(*)" in sql:
                    return FakeResult(
                        {"owner": "CLX", "object_name": name, "column_count": 1}
                        for name in normal_names + partition_names
                    )
                if "all_constraints" in sql:
                    return FakeResult(
                        {"owner": "CLX", "object_name": name, "column_name": "ID", "position": 1}
                        for name in normal_names + partition_names
                    )
                if "all_sequences" in sql:
                    return FakeResult([])
                raise AssertionError(f"unexpected statement: {statement}")

            @staticmethod
            def assert_owners(parameters):
                if parameters != {"owners": ["CLX"]}:
                    raise AssertionError(f"unexpected owners: {parameters}")

        connection = FakeConnection()
        engine = SimpleNamespace(
            dialect=SimpleNamespace(name="oracle"),
            connect=lambda: connection,
        )
        objects = list_objects(engine, "CLX", owners=["clx"])
        types = {item["name"]: item["object_type"] for item in objects}

        self.assertEqual(sum(kind == "table" for kind in types.values()), 40)
        self.assertEqual(
            sum(kind == "partitioned_table" for kind in types.values()), 8
        )
        self.assertTrue(all(types[name] == "table" for name in normal_names))
        self.assertTrue(
            all(types[name] == "partitioned_table" for name in partition_names)
        )
        self.assertEqual(connection.calls, 4)
        self.assertTrue(all(item["columns"] == 1 for item in objects))
        self.assertTrue(all(item["primary_keys"] == ["ID"] for item in objects))

    def test_numeric_precision_is_preserved(self):
        mapped = portable_type(Numeric(18, 4))
        self.assertIsInstance(mapped, Numeric)
        self.assertEqual((mapped.precision, mapped.scale), (18, 4))

    def test_long_strings_remain_strings(self):
        mapped = portable_type(String(512))
        self.assertIsInstance(mapped, String)
        self.assertEqual(mapped.length, 512)

    def test_binary_and_datetime(self):
        self.assertIsInstance(portable_type(LargeBinary()), LargeBinary)
        self.assertIsInstance(portable_type(DateTime()), DateTime)

    def test_unknown_becomes_text(self):
        class VendorSpecific:
            pass
        self.assertIsInstance(portable_type(VendorSpecific()), Text)

    def test_mysql_lob_and_identity_mapping(self):
        self.assertIsInstance(portable_type(Text(), "mysql"), mysql.LONGTEXT)
        self.assertIsInstance(portable_type(LargeBinary(), "mysql"), mysql.LONGBLOB)
        self.assertIsInstance(portable_type(Numeric(), "mysql", identity=True), BigInteger)
        number = portable_type(oracle.NUMBER(38, 0), "mysql")
        self.assertIsInstance(number, Numeric)
        self.assertEqual((number.precision, number.scale), (38, 0))
        self.assertIsInstance(portable_type(oracle.RAW(2000), "mysql"), mysql.VARBINARY)
        self.assertIsInstance(portable_type(oracle.CHAR(20), "mysql"), mysql.CHAR)

    def test_oracle_binary_double_uses_mysql_double(self):
        mapped = portable_type(oracle.BINARY_DOUBLE(), "mysql")
        self.assertIsInstance(mapped, mysql.DOUBLE)
        self.assertFalse(mapped.asdecimal)

        oracle_float = portable_type(oracle.FLOAT(), "mysql")
        self.assertIsInstance(oracle_float, mysql.DOUBLE)

        dynamic_number = portable_type(oracle.NUMBER(), "mysql")
        self.assertIsInstance(dynamic_number, mysql.NUMERIC)
        self.assertEqual((dynamic_number.precision, dynamic_number.scale), (65, 0))

    def test_oracle_number_uses_tdsql_safe_integer_thresholds(self):
        self.assertIsInstance(portable_type(oracle.NUMBER(2, 0), "mysql"), mysql.TINYINT)
        self.assertIsInstance(portable_type(oracle.NUMBER(4, 0), "mysql"), mysql.SMALLINT)
        self.assertIsInstance(portable_type(oracle.NUMBER(9, 0), "mysql"), Integer)
        self.assertIsInstance(portable_type(oracle.NUMBER(18, 0), "mysql"), BigInteger)
        wide = portable_type(oracle.NUMBER(19, 0), "mysql")
        self.assertIsInstance(wide, mysql.NUMERIC)
        self.assertEqual((wide.precision, wide.scale), (19, 0))

    def test_oracle_number_outside_tdsql_decimal_range_becomes_text(self):
        high_scale = portable_type(oracle.NUMBER(38, 31), "mysql")
        negative_scale = portable_type(oracle.NUMBER(38, -30), "mysql")
        self.assertIsInstance(high_scale, String)
        self.assertIsInstance(negative_scale, String)

    @patch(
        "app.database._oracle_column_type_details",
        return_value={"data_type": "NUMBER", "precision": 10, "scale": 0},
    )
    @patch("app.database._oracle_column_data_type", return_value="NUMBER")
    def test_range_hash_subpartition_clause_uses_tdsql_order(self, _type, _details):
        info = {
            "partitioning_type": "RANGE",
            "subpartitioning_type": "HASH",
            "def_subpartition_count": 4,
            "partition_key_columns": ["ID"],
            "subpartition_key_columns": ["REGION_CODE"],
            "partitions": [
                {"name": "P1", "high_value": "100"},
                {"name": "P2", "high_value": "MAXVALUE"},
            ],
        }
        clause, warnings, _ = build_partition_clause(
            info,
            SimpleNamespace(),
            "CLX",
            "TST_PART_RANGE_HASH",
            "mysql",
            primary_keys=["ID", "REGION_CODE"],
        )
        self.assertIn(
            "PARTITION BY RANGE (`ID`) SUBPARTITION BY HASH (`REGION_CODE`) "
            "SUBPARTITIONS 4 (PARTITION",
            clause,
        )
        self.assertTrue(warnings)

    @patch(
        "app.database._oracle_column_type_details",
        return_value={"data_type": "NUMBER", "precision": 38, "scale": 0},
    )
    def test_unsafe_wide_numeric_partition_key_is_blocked(self, _details):
        info = {
            "partitioning_type": "RANGE",
            "subpartitioning_type": "NONE",
            "partition_key_columns": ["ID"],
            "partitions": [{"name": "P1", "high_value": "MAXVALUE"}],
        }
        clause, warnings, _ = build_partition_clause(
            info,
            SimpleNamespace(),
            "CLX",
            "WIDE_PARTITION",
            "mysql",
            primary_keys=["ID"],
        )
        self.assertEqual(clause, "")
        self.assertIn("BIGINT", warnings[0])

    def test_partition_ddl_explicitly_uses_innodb_and_utf8mb4(self):
        target = create_engine("mysql+pymysql://")
        table = Table(
            "probe_partition",
            MetaData(),
            Column("id", BigInteger, nullable=False),
            Column("name", String(100)),
        )
        ddl = _render_partitioned_create_ddl(
            table,
            target,
            "PARTITION BY HASH (`id`) PARTITIONS 4",
            primary_key_columns=["id"],
            partition_key_columns=["id"],
        )
        self.assertIn("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", ddl)
        self.assertIn("PARTITION BY HASH (`id`) PARTITIONS 4", ddl)

    def test_hash_normalizes_source_number_to_target_float_precision(self):
        target_type = mysql.DOUBLE(asdecimal=False)
        self.assertEqual(
            canonical(Decimal("0.142857142857142857142857"), target_type),
            canonical(0.14285714285714285, target_type),
        )

    def test_hash_ignores_only_fixed_character_right_padding(self):
        self.assertEqual(
            canonical("T14-000000  ", mysql.CHAR(12)),
            canonical("T14-000000", mysql.CHAR(12)),
        )
        self.assertEqual(
            canonical("中文-FlowDB          ", oracle.NCHAR(20)),
            canonical("中文-FlowDB", oracle.NCHAR(20)),
        )
        self.assertNotEqual(
            canonical(" T14-000000", mysql.CHAR(12)),
            canonical("T14-000000", mysql.CHAR(12)),
        )

    def test_hash_preserves_variable_character_right_padding(self):
        self.assertNotEqual(
            canonical("T14-000000  ", String(20)),
            canonical("T14-000000", String(20)),
        )
        self.assertNotEqual(
            canonical("正文  ", Text()),
            canonical("正文", Text()),
        )

    def test_copy_batches_moves_real_rows(self):
        source = create_engine("sqlite://")
        target = create_engine("sqlite://")
        metadata = MetaData()
        customers = Table("customers", metadata, Column("id", Integer, primary_key=True), Column("name", String(80)))
        metadata.create_all(source)
        with source.begin() as connection:
            connection.execute(customers.insert(), [{"id": index, "name": f"客户-{index}"} for index in range(1, 2506)])
        prepared = prepare_table(source, target, None, None, "customers", "fail", True)
        copied = sum(batch[0] for batch in copy_batches(source, target, prepared, 500))
        with target.connect() as connection:
            count = connection.scalar(select(func.count()).select_from(prepared.target))
        self.assertEqual(copied, 2505)
        self.assertEqual(count, 2505)
        source.dispose()
        target.dispose()


if __name__ == "__main__":
    unittest.main()
