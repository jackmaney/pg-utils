import sys
sys.path = ['..'] + sys.path

import unittest
import pg_utils
import os

# Override to create the test table in a schema other than your own.
user_schema = os.getenv("pg_username")

conn = None

def setUpModule():
    global conn
    conn = pg_utils.connection.Connection()

def tearDownModule():
    global conn
    if conn is not None:
        conn.close()

class TestTableColumnSelect(unittest.TestCase):

    def test_column_select(self):

        table_name = "pg_utils_test"
        table = pg_utils.table.Table.create(conn, user_schema, table_name,
                                            """create table {}.{} as
          select random() as x, random() as y
          from generate_series(1,100)
          distributed by (x,y);""".format(user_schema, table_name))

        self.assertEqual(table.count, 100)
        self.assertEqual(table.shape, (100, 2))

        self.assertEqual(set(table.columns), {"x", "y"})
        self.assertEqual(set(table.columns), set(table.all_columns))

        sub_table = table["x"]

        self.assertEqual(len(sub_table.columns), 1)
        self.assertEqual(sub_table.columns, ["x"])
        self.assertNotEqual(sub_table.columns, sub_table.all_columns)

        self.assertEqual(sub_table.count, 100)

        table.drop()