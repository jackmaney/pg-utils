import sys

sys.path = ['..'] + sys.path

import unittest
from pg_utils import table
import pandas as pd

table_name = "pg_utils_test_dtypes"


class TestDtypes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table = table.Table.create(table_name,
                                       """create table {} as
     select x::int as x, random() as y, random() as z, 'abc'::text as w
     from generate_series(1,100) x
     distributed by (x,y);""".format(table_name))

    @classmethod
    def tearDownClass(cls):
        cls.table.drop()

    def test_dtypes(self):
        dtypes = self.table.dtypes

        self.assertTrue(isinstance(dtypes, pd.Series))
        self.assertEqual(list(dtypes.index), list("xyzw"))
        self.assertEqual(list(dtypes),
                         ["integer", "double precision", "double precision", "text"])

        dtype_counts = self.table.get_dtype_counts()

        self.assertTrue(isinstance(dtype_counts, pd.Series))
        self.assertEqual(list(dtype_counts.index),
                         ["double precision", "integer", "text"])
        self.assertEqual(list(dtype_counts), [2, 1, 1])
