import sys
import unittest

import pandas as pd

sys.path = ['..'] + sys.path

from pg_utils import table

table_name = "pg_utils_test_sort_values"

t = table.Table.create(table_name,
                       """create table {} as
                       select 3 as x, 'b' as y
                       union all
                       select 1 as x, 'c' as y
                       union all
                       select 2 as x, 'a' as y
                       union all
                       select 4 as x, 'a' as y
                       union all
                       select 2 as x, 'd' as y""".format(table_name))


class TestSortValues(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        t.drop()

    def test_sort_values_column(self):
        x_sorted = pd.Series([1, 2, 2, 3, 4])
        y_sorted = pd.Series(['a', 'a', 'b', 'c', 'd'])

        x_desc = pd.Series([4, 3, 2, 2, 1])
        y_desc = pd.Series(['d', 'c', 'b', 'a', 'a'])

        self.assertTrue(t.x.sort_values().equals(x_sorted))
        self.assertTrue(t.y.sort_values().equals(y_sorted))

        self.assertTrue(t.x.sort_values(ascending=False).equals(x_desc))
        self.assertTrue(t.y.sort_values(ascending=False).equals(y_desc))

    def test_sort_values_frame(self):
        self.assertTrue(t.sort_values("x").x.equals(pd.Series([1, 2, 2, 3, 4])))
        self.assertTrue(t.sort_values("y").y.equals(pd.Series(['a', 'a', 'b', 'c', 'd'])))

        expected_xy = pd.DataFrame(
            {"x": [1, 2, 2, 3, 4],
             "y": ['c', 'a', 'd', 'b', 'a']},
            columns=["x", "y"])

        self.assertTrue(t.sort_values(["x", "y"]).equals(expected_xy))

        expected_xyd = pd.DataFrame(
            {"x": [1, 2, 2, 3, 4],
             "y": ['c', 'd', 'a', 'b', 'a']},
            columns=["x", "y"])

        self.assertTrue(t.sort_values(["x", "y"], ascending=[True, False]).equals(expected_xyd))

        expected_xdy = pd.DataFrame(
            {
                "x": [4, 3, 2, 2, 1],
                "y": ['a', 'b', 'a', 'd', 'c']
            },
            columns=["x", "y"]
        )

        self.assertTrue(t.sort_values(["x", "y"], ascending=[False, True]).equals(expected_xdy))

        expected_xdyd = pd.DataFrame(
            {
                "x": [4, 3, 2, 2, 1],
                "y": ['a', 'b', 'd', 'a', 'c']
            },
            columns=["x", "y"]
        )

        self.assertTrue(t.sort_values(["x", "y"], ascending=False).equals(expected_xdyd))

        expected_yx = pd.DataFrame(
            {"x": [2, 4, 3, 1, 2],
             "y": ['a', 'a', 'b', 'c', 'd']},
            columns=["x", "y"])
        self.assertTrue(t.sort_values(["y", "x"]).equals(expected_yx))

        expected_yxd = pd.DataFrame(
            {"x": [4, 2, 3, 1, 2],
             "y": ['a', 'a', 'b', 'c', 'd']},
            columns=["x", "y"])

        self.assertTrue(t.sort_values(["y", "x"], ascending=[True, False]).equals(expected_yxd))

        expected_ydx = pd.DataFrame(
            {"x": [2, 1, 3, 2, 4],
             "y": ['d', 'c', 'b', 'a', 'a']},
            columns=["x", "y"])

        self.assertTrue(t.sort_values(["y", "x"], ascending=[False, True]).equals(expected_ydx))

        expected_ydxd = pd.DataFrame(
            {"x": [2, 1, 3, 4, 2],
             "y": ['d', 'c', 'b', 'a', 'a']},
            columns=["x", "y"])

        self.assertTrue(t.sort_values(["y", "x"], ascending=False).equals(expected_ydxd))
