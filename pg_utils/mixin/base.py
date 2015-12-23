from lazy_property import LazyProperty
import pandas as pd

class SelectAllMixIn(object):


    def select_all_query(self, override_order_by=False):

        query = "select * from {}".format(self)

class CountableMixIn(object):

    def __init__(self, conn):

        self.conn = conn

    @LazyProperty
    def count(self):
        """Returns the number of rows in the corresponding database table."""
        cur = self.conn.cursor()

        cur.execute("select count(1) from {}".format(self))

        return cur.fetchone()[0]