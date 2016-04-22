import sys

sys.path = ['..'] + sys.path

import unittest
from pg_utils import connection, table, column

# Override to create the test table in a schema other than your own.
table_name = "pg_utils_test_column_unique"

t = table.Table.create(table_name, """create table {} as select generate_series(1,10) x, 'a' as y distributed by (x)""".format(table_name))

class TestColumnUnique(unittest.TestCase):

    def test_is_unique(self):

        self.assertTrue(t.x.is_unique)
        self.assertFalse(t.y.is_unique)