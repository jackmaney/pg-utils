import sys

sys.path = ['..'] + sys.path

import unittest
import pg_utils
import os
import pandas as pd

# Override to create the test table in a schema other than your own.
user_schema = os.getenv("pg_username")
table_name = "pg_utils_test_column_attribute"

class TestColumnAttribute(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = pg_utils.connection.Connection()
        cls.table = pg_utils.table.Table.create(cls.conn, user_schema, table_name,
                                            """create table {}.{} as
          select random() as x, random() as y
          from generate_series(1,100)
          distributed by (x,y);""".format(user_schema, table_name))

    @classmethod
    def tearDownClass(cls):
        if pg_utils.table.Table.exists(cls.conn, user_schema, table_name):
            cls.table.drop()

        cls.conn.close()

    def test_column_attribute(self):
        table_y = self.table.y

        self.assertTrue(table_y)
        self.assertEqual(table_y.columns, ["y"])
        y_col = self.table.y.head("all")
        self.assertTrue(isinstance(y_col, pd.Series))

        y_values = [x for x in y_col]
        self.assertEqual(len(y_values), 100)
        self.assertTrue(all([0<= x <=1 for x in y_values]))


