import sys

sys.path = ['..'] + sys.path

import unittest
from pg_utils import connection, table
import os
import numpy as np

# Override to create the test table in a schema other than your own.
user_schema = os.getenv("pg_username")
table_name = "pg_utils_test_column_attribute"


class TestColumnAttribute(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connection.Connection()
        cls.table = table.Table.create(cls.conn, user_schema, table_name,
                                       """create table {}.{} as
     select random() as x, random() as y
     from generate_series(1,100)
     distributed by (x,y);""".format(user_schema, table_name))

    @classmethod
    def tearDownClass(cls):
        if table.Table.exists(cls.conn, user_schema, table_name):
            cls.table.drop()

        cls.conn.close()

    def test_column_attribute(self):
        table_y = self.table.y

        self.assertTrue(table_y)
        y_values = self.table.y.values
        self.assertTrue(isinstance(y_values, np.ndarray))
        self.assertEqual(len(y_values), 100)
        self.assertTrue(all([0 <= x <= 1 for x in y_values]))
