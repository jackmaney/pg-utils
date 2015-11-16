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


class TestTableBasic(unittest.TestCase):
    def test_created_basic(self):
        table_name = "pg_utils_test"
        table = pg_utils.table.Table.create(conn, user_schema, table_name,
                                            """create table {}.{} as
          select generate_series(1,10) as x
          distributed by (x);""".format(user_schema, table_name))

        self.assertTrue(table)
        self.assertTrue(pg_utils.table.Table.exists(conn, user_schema, table_name))

        self.assertEqual(table.count, 10)
        self.assertEqual(table.shape, (10, 1))

        self.assertEqual({n for n in table.head("all").x}, set(range(1,11)))

        table.drop()

        self.assertFalse(pg_utils.table.Table.exists(conn, user_schema, table_name))


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
        self.assertEqual(sub_table.shape, (100, 1))

        table.drop()
