import sys

sys.path = ['..'] + sys.path

import unittest
from pg_utils import connection, table
import os

_has_seaborn=True

try:
    import seaborn
except ImportError:
    _has_seaborn = False

# Override to create the test table in a schema other than your own.
user_schema = os.getenv("pg_username")
table_name = "pg_utils_test_distplot"


# noinspection PyBroadException
@unittest.skipIf(not _has_seaborn, "seaborn not found, or there was an issue importing it.")
class TestDistPlot(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connection.Connection()
        cls.table = table.Table.create(cls.conn, user_schema, table_name,
                                                      """
                                                      create table {}.{} as
                                                      select random() as x from generate_series(1, 100)
                                                      distributed by (x)""".format(user_schema, table_name))

    @classmethod
    def tearDownClass(cls):
        if table.Table.exists(cls.conn, user_schema, table_name):
            cls.table.drop()

        cls.conn.close()

    def test_table_exists(self):
        self.assertTrue(table.Table.exists(self.conn, user_schema, table_name))

    def test_distplot_basic(self):
        good = True

        self.table.x.distplot(bins=10)

        # try:
        #     self.table.x.distplot(bins=10)
        # except:
        #     good = False

        self.assertTrue(good)

    def test_displot_freedman_diaconis(self):
        good = True

        self.table.x.distplot()

        # try:
        #     self.table.x.distplot()
        # except:
        #     good = False

        self.assertTrue(good)
