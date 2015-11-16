import sys

sys.path = ['..'] + sys.path

import unittest
import pg_utils
import os

# Override to create the test table in a schema other than your own.
user_schema = os.getenv("pg_username")
table_name = "pg_utils_test_distplot"


# noinspection PyBroadException
class TestDistPlot(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = pg_utils.connection.Connection()
        cls.table = pg_utils.table.Table.create(cls.conn, user_schema, table_name,
                                        """
                                        create table {}.{} as
                                        select random() as x from generate_series(1, 100)
                                        distributed by (x)""".format(user_schema, table_name))

    @classmethod
    def tearDownClass(cls):
        if pg_utils.table.Table.exists(cls.conn, user_schema, table_name):
            cls.table.drop()

        cls.conn.close()

    def test_table_exists(self):

        self.assertTrue(pg_utils.table.Table.exists(self.conn, user_schema, table_name))

    def test_distplot_basic(self):

        good = True

        try:
            self.table.distplot("x", bins=10)
        except:
            good = False

        self.assertTrue(good)

    def test_displot_freedman_diaconis(self):

        good = True

        # try:
        #     self.table.distplot("x")
        # except:
        #     good = False

        self.table.distplot("x")

        self.assertTrue(good)


