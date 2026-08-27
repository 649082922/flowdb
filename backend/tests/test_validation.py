from __future__ import annotations

import unittest

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select

from app.validation import compare_and_hash_queries, resolve_table_name


class ValidationStreamingTests(unittest.TestCase):
    def setUp(self):
        self.source = create_engine("sqlite://")
        self.target = create_engine("sqlite://")
        for engine in (self.source, self.target):
            metadata = MetaData()
            table = Table(
                "items",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(50), nullable=False),
            )
            metadata.create_all(engine)
            with engine.begin() as connection:
                connection.execute(
                    table.insert(),
                    [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
                )

    def tearDown(self):
        self.source.dispose()
        self.target.dispose()

    def _compare(self):
        source_table = Table("items", MetaData(), autoload_with=self.source)
        target_table = Table("items", MetaData(), autoload_with=self.target)
        with self.source.connect() as source_connection, self.target.connect() as target_connection:
            return compare_and_hash_queries(
                source_connection,
                target_connection,
                select(source_table).order_by(source_table.c.id),
                select(target_table).order_by(target_table.c.id),
                ["id", "name"],
                ["id"],
                {"id": target_table.c.id.type, "name": target_table.c.name.type},
                fetch_size=1,
            )

    def test_equal_streams_have_equal_hashes_without_differences(self):
        source_count, source_hash, target_count, target_hash, differences, samples, columns = self._compare()
        self.assertEqual((source_count, target_count), (2, 2))
        self.assertEqual(source_hash, target_hash)
        self.assertEqual(differences, 0)
        self.assertEqual(samples, [])
        self.assertEqual(columns, {})

    def test_mismatch_is_hashed_and_sampled_in_same_pass(self):
        target_table = Table("items", MetaData(), autoload_with=self.target)
        with self.target.begin() as connection:
            connection.execute(
                target_table.update().where(target_table.c.id == 2).values(name="changed")
            )
        _, source_hash, _, target_hash, differences, samples, columns = self._compare()
        self.assertNotEqual(source_hash, target_hash)
        self.assertEqual(differences, 1)
        self.assertEqual(columns, {"name": 1})
        self.assertEqual(samples[0]["primary_key"], {"id": ["number", "2"]})

    def test_cached_object_names_are_used_case_insensitively(self):
        self.assertEqual(
            resolve_table_name(self.target, None, "ITEMS", ["items"]),
            ("items", False),
        )


if __name__ == "__main__":
    unittest.main()
