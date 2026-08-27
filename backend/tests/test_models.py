import unittest

from pydantic import ValidationError

from app.models import CreateJobRequest


class IndependentMigrationObjectTests(unittest.TestCase):
    def test_sequence_only_job_is_valid(self):
        request = CreateJobRequest(
            name="sequence-only",
            link_id="saved-link",
            tables=[],
            sequences=["ISEQ$$_76238"],
            migrate_sequences=True,
            migration_content="structure_only",
        )
        self.assertEqual(request.tables, [])
        self.assertEqual(request.sequences, ["ISEQ$$_76238"])

    def test_table_only_job_is_valid(self):
        request = CreateJobRequest(
            name="table-only",
            link_id="saved-link",
            tables=["CLX_NUMERIC_01"],
            object_types={"CLX_NUMERIC_01": "table"},
            sequences=[],
            migrate_sequences=False,
        )
        self.assertEqual(request.object_types["CLX_NUMERIC_01"], "table")

    def test_view_only_job_is_valid(self):
        request = CreateJobRequest(
            name="view-only",
            link_id="saved-link",
            tables=["CLX_V_MIGRATION_SAMPLE"],
            object_types={"CLX_V_MIGRATION_SAMPLE": "view"},
            sequences=[],
            migrate_sequences=False,
            migration_content="structure_only",
        )
        self.assertEqual(request.object_types["CLX_V_MIGRATION_SAMPLE"], "view")

    def test_disabled_sequences_do_not_make_an_empty_job_valid(self):
        with self.assertRaises(ValidationError):
            CreateJobRequest(
                name="empty",
                link_id="saved-link",
                tables=[],
                sequences=["ISEQ$$_76238"],
                migrate_sequences=False,
            )


if __name__ == "__main__":
    unittest.main()
