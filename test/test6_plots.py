import sys
import unittest

sys.path = ['..'] + sys.path

from pg_utils import table

_has_seaborn = True

try:
    import seaborn
except ImportError:
    _has_seaborn = False

# Override to create the test table in a schema other than your own.
table_name = "pg_utils_test_column_plot"

t = table.Table.create(table_name,
                       """create table {} as select random() as x from generate_series(1,100) distributed by (x)""".format(
                           table_name))


@unittest.skipIf(not _has_seaborn, "seaborn not found, or there was an issue importing it.")
class TestPlots(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        t.drop()

    def test_plots(self):
        for name in ["line", "area", "bar", "barh", "box", "density", "hist", "kde", "pie"]:
            self.assertTrue(getattr(t.x.plot, name)(), msg="Problem with {}".format(name))
