import sys

import pg_utils.table.table

sys.path = ['..'] + sys.path

import unittest
import pg_utils
import os

# Override to create the test table in a schema other than your own.
user_schema = os.getenv("pg_username")
table_name = "pg_utils_test_basic"

conn = pg_utils.connection.Connection()



t1 = pg_utils.table.table.Table.create(conn, user_schema, "{}_1".format(table_name),
                                            """create table {}.{} as
          select generate_series(1,10) as x
          distributed by (x);""".format(user_schema, "{}_1".format(table_name)))

t2 = pg_utils.table.table.Table.create(conn, user_schema, "{}_2".format(table_name),
                                            """create table {}.{} as
          select random() as x, random() as y, random() as z
          from generate_series(1,100)
          distributed by (x,y);""".format(user_schema, "{}_2".format(table_name)))

def tearDownModule():
    t1.drop()
    t2.drop()

    global conn
    if conn is not None:
        conn.close()

class TestTableBasic(unittest.TestCase):
    def test_created_basic(self):

        self.assertTrue(t1)
        self.assertTrue(pg_utils.table.table.Table.exists(conn, t1.schema, t1.table_name))

        self.assertEqual(t1.count, 10)
        self.assertEqual(t1.shape, (10, 1))

        self.assertEqual({n for n in t1.head("all")}, set(range(1,11)))


class TestTableColumnsSelect(unittest.TestCase):
    def test_columns_select(self):


        self.assertEqual(t2.count, 100)
        self.assertEqual(t2.shape, (100, 3))

        self.assertEqual(set([str(x) for x in t2.columns]), {"x", "y", "z"})
        self.assertEqual(set([str(x) for x in t2.columns]),
                         set([str(x) for x in t2.all_columns]))

        sub_table = t2[["x", "y"]]

        self.assertEqual(len(sub_table.columns), 2)
        self.assertEqual(set([str(x) for x in sub_table.columns]), {"x", "y"})
        self.assertNotEqual(set([str(x) for x in sub_table.columns]),
                            set([str(x) for x in sub_table.all_columns]))

        self.assertEqual(sub_table.count, 100)
        self.assertEqual(sub_table.shape, (100, 2))


class TestTableSingleColumnSelect(unittest.TestCase):

    def test_table_single_column_select(self):

        col = t2.x

        self.assertTrue(isinstance(col, pg_utils.column.Column))

        self.assertEqual(col.size, 100)

        self.assertTrue(col.max <= 1)
        self.assertTrue(col.min >= 0)
