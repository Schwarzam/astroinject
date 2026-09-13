import unittest

from astropy.table import Table

from astroinject.database.gen_base_queries import generate_create_table_query
from astroinject.database.gen_index_queries import make_pg_sphere_index, make_q3c_index
from astroinject.database.tablespaces import get_tablespace


class TablespaceQueryTests(unittest.TestCase):
    def setUp(self):
        self.table = Table({"id": [1], "ra": [10.0], "dec": [-1.0]})

    def test_create_table_places_data_and_primary_key_in_separate_tablespaces(self):
        query = generate_create_table_query(
            "catalog.sources", self.table, "id", "pg_default", "storage"
        )

        self.assertIn("PRIMARY KEY USING INDEX TABLESPACE storage", query)
        self.assertTrue(query.endswith("TABLESPACE pg_default;"))

    def test_indexes_use_configured_tablespace(self):
        pgsphere = make_pg_sphere_index("catalog.sources", "ra", "dec", "storage")
        q3c, _ = make_q3c_index("catalog.sources", "ra", "dec", "storage")

        self.assertIn("TABLESPACE storage;", pgsphere)
        self.assertIn("TABLESPACE storage;", q3c)

    def test_absent_configuration_keeps_postgres_defaults(self):
        self.assertIsNone(get_tablespace({}, "table"))
        query = generate_create_table_query("sources", self.table, "id")
        self.assertNotIn("TABLESPACE", query)


if __name__ == "__main__":
    unittest.main()
