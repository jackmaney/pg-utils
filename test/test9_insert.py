import os
import sys
import unittest

sys.path = ['..'] + sys.path

from pg_utils import table
import pandas as pd

t = table.Table.create("pg_utils_insert_test",
                       """create table pg_utils_insert_test
(x int, y text)
distributed randomly;""")


class TestInsert(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        t.drop()

    def test_basic_insert(self):
        t.insert([1, "a"])
        self.assertEqual(t.count, 1)
        self.assertTrue(t.head().equals(pd.DataFrame([[1, "a"]], columns=["x", "y"])))

    def test_insert_csv(self):
        try:
            with open("test.csv", "w") as f:
                import csv
                writer = csv.writer(f, delimiter=",", lineterminator="\n")
                writer.writerow(["x", "y"])
                writer.writerow([2, "b"])
                writer.writerow([3, "c"])

            t.insert_csv("test.csv")
            if hasattr(t, "_count"):
                delattr(t, "_count")

            self.assertTrue(t.count, 3)

            self.assertTrue(t.head().sort_values("x").reset_index(drop=True).equals(
                pd.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["x", "y"])))

        finally:

            if os.path.exists("test.csv"):
                os.remove("test.csv")

    def test_insert_dataframe(self):

        df = pd.DataFrame([[4, "d"], [5, "e"], [6, "f"], [7, "g"]], columns=["x", "y"])

        t.insert_dataframe(df)
        if hasattr(t, "_count"):
            delattr(t, "_count")

        self.assertEqual(t.count, 7)
        self.assertTrue(t.head().sort_values("x").reset_index(drop=True).equals(
            pd.DataFrame([[1, "a"], [2, "b"], [3, "c"],
                          [4, "d"], [5, "e"], [6, "f"], [7, "g"]], columns=["x", "y"]))
        )
