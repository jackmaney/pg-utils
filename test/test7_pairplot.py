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
table_name = "pg_utils_test_pairplot"

t = table.Table.create(table_name,
                       """create table {} as
                       select random() as x,
                       sqrt(-2 * ln(u1)) * cos(2 * PI() * u2) as y
                       from (
                           select random() as u1,
                           random() as u2
                           from generate_series(1, 10000)
                     )a""".format(table_name))


@unittest.skipIf(not _has_seaborn, "seaborn not found, or there was an issue importing it.")
class TestPairPlot(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        t.drop()

    def test_pairplot(self):
        self.assertTrue(t.pairplot())
