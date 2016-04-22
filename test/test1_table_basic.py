import sys

sys.path = ['..'] + sys.path

import unittest
from pg_utils import connection, table, column

# Override to create the test table in a schema other than your own.
table_name = "pg_utils_test_basic"

conn = connection.Connection()

t1 = table.Table.create("{}_1".format(table_name),
                        """create table {} as
select generate_series(1,10) as x
distributed by (x);""".format("{}_1".format(table_name)), conn=conn)

t2 = table.Table.create("{}_2".format(table_name),
                        """create table {} as
select random() as x, random() as y, random() as z
from generate_series(1,100)
distributed by (x,y);""".format("{}_2".format(table_name)), conn=conn)


def tearDownModule():
    t1.drop()
    t2.drop()


class TestTableBasic(unittest.TestCase):
    def test_created_basic(self):
        self.assertTrue(t1)
        self.assertTrue(table.Table.exists(t1.table_name, schema=t1.schema, conn=conn))

        self.assertEqual(t1.count, 10)
        self.assertEqual(t1.shape, (10, 1))

        self.assertEqual({n for n in t1.head("all")}, set(range(1, 11)))


class TestTableColumnsSelect(unittest.TestCase):
    def test_columns_select(self):
        self.assertEqual(t2.count, 100)
        self.assertEqual(t2.shape, (100, 3))

        self.assertEqual(set([str(x) for x in t2.columns]), {"x", "y", "z"})
        self.assertEqual(set([str(x) for x in t2.columns]),
                         set([str(x) for x in t2.all_columns]))

        sub_table = t2[["x", "y"]]

        self.assertEqual(len(sub_table.columns), 2)
        self.assertEqual({str(x) for x in sub_table.columns}, {"x", "y"})
        self.assertNotEqual({str(x) for x in sub_table.columns},
                            {str(x) for x in sub_table.all_columns})

        self.assertEqual(sub_table.count, 100)
        self.assertEqual(sub_table.shape, (100, 2))


class TestTableSingleColumnSelect(unittest.TestCase):
    def test_table_single_column_select(self):
        col = t2.x

        self.assertTrue(isinstance(col, column.Column))

        self.assertEqual(col.size, 100)

        self.assertTrue(col.max <= 1)
        self.assertTrue(col.min >= 0)
