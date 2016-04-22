import sys

sys.path = ['..'] + sys.path

import unittest
from pg_utils import table

_has_seaborn = True

try:
    import seaborn
except ImportError:
    _has_seaborn = False

# Override to create the test table in a schema other than your own.
table_name = "pg_utils_test_distplot"

test_table = table.Table.create(table_name,
                                """
                                create table {} as
                                select random() as x from generate_series(1, 100)
                                distributed by (x)""".format(table_name))


# noinspection PyBroadException
@unittest.skipIf(not _has_seaborn, "seaborn not found, or there was an issue importing it.")
class TestDistPlot(unittest.TestCase):
    test_table = test_table

    @classmethod
    def tearDownClass(cls):
        test_table.drop()

    def test_table_exists(self):
        self.assertTrue(table.Table.exists(test_table.name, conn=test_table.conn))

    def test_distplot_basic(self):
        self.assertTrue(test_table.x.distplot(bins=10))

    def test_displot_freedman_diaconis(self):
        self.assertTrue(test_table.x.distplot())
