import sys

sys.path = ['..'] + sys.path

import unittest
from pg_utils import table
import numpy as np

table_name = "pg_utils_test_column_attribute"


class TestColumnAttribute(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table = table.Table.create(table_name,
                                       """create table {} as
     select random() as x, random() as y
     from generate_series(1,100)
     distributed by (x,y);""".format(table_name))

    @classmethod
    def tearDownClass(cls):
        cls.table.drop()

    def test_column_attribute(self):
        table_y = self.table.y

        self.assertTrue(table_y)
        y_values = self.table.y.values
        self.assertTrue(isinstance(y_values, np.ndarray))
        self.assertEqual(len(y_values), 100)
        self.assertTrue(all([0 <= x <= 1 for x in y_values]))
