import numpy as np
import pandas as pd
from lazy_property import LazyProperty

import pg_utils.table.table
from . import _describe_template
from .. import bin_counts
from .. import numeric_datatypes, _pretty_print


class Column(object):
    def __init__(self, name, parent_table, sort=False):
        """

        :param str name:
        :param pg_utils.table.Table parent_table:
        :param str|bool sort: Either ``"desc"`` to sort descending, any other truthy value
        to sort ascending, or any false value to not sort.
        """

        if not isinstance(parent_table, pg_utils.table.table.Table):
            raise ValueError(
                "The 'parent_table' parameter must be a pg_utils.parent_table.Table, not {}".format(
                    type(parent_table)
                ))

        self.parent_table = parent_table
        self.name = name
        self.is_numeric = parent_table.all_column_data_types[name] in numeric_datatypes
        self.sort = sort

    def select_all_query(self):

        query = "select {} from {}".format(self, self.parent_table)

        if self.sort:
            query += " order by 1"

            if self.sort == "desc":
                query += " desc"

        return query

    @LazyProperty
    def dtype(self):
        return self.parent_table.all_column_data_types[self.name]

    def _get_describe_query(self, percentiles=None, type_="continuous"):

        if type_.lower() not in ["continuous", "discrete"]:
            raise ValueError("The 'type_' parameter must be 'continuous' or 'discrete'")

        if not self.is_numeric:
            return None

        if percentiles is None:
            percentiles = [0.25, 0.5, 0.75]
        elif not bool(percentiles):
            percentiles = []

        if not isinstance(percentiles, (list, tuple)):
            percentiles = [percentiles]

        if any([x < 0 or x > 1 for x in percentiles]):
            raise ValueError(
                "The `percentiles` attribute must be None or consist of numbers between 0 and 1 (got {})".format(
                    percentiles))

        percentiles = sorted([float("{0:.2f}".format(p)) for p in percentiles if p > 0])

        suffix = "cont" if type_.lower() == "continuous" else "desc"

        query = _describe_template.render(column=self, percentiles=percentiles,
                                          suffix=suffix, table=self.parent_table)

        if self.parent_table.debug:
            _pretty_print(query)

        return query

    def describe(self, percentiles=None, type_="continuous"):

        if percentiles is None:
            percentiles = [0.25, 0.5, 0.75]

        cur = self.parent_table.conn.cursor()

        cur.execute(self._get_describe_query(percentiles=percentiles, type_=type_))

        index = ["count", "mean", "std_dev", "minimum"] + \
                ["{}%".format(int(100 * p)) for p in percentiles] + \
                ["maximum"]

        return pd.Series(cur.fetchone()[1:], index=index)

    def distplot(self, bins=None, **kwargs):
        """
        Produces a ``distplot``. See `the seaborn docs <http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.distplot.html>`_ on ``distplot`` for more information.

        :param int|None bins: Either a positive integer number of bin_counts to use.
        :param dict kwargs: A dictionary of options to pass on to `seaborn.distplot <http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.distplot.html>`_.
        """

        try:
            import seaborn
            import numpy as np
        except ImportError as e:
            raise ImportError(
                "You do not have seaborn installed (or there was an issue importing it). Please install seaborn (or pg-utils[graphics])")

        bc = bin_counts.counts(self, bins=bins)

        n = sum([entry[2] for entry in bc])

        left = np.zeros(n)
        right = np.zeros(n)

        overall_index = 0
        for entry in bc:
            for i in range(entry[2]):
                left[overall_index] = entry[0]
                right[overall_index] = entry[1]
                overall_index += 1

        # We'll take our overall data points to be in the midpoint
        # of each binning interval
        # TODO: make this more configurable (left, right, etc)
        return seaborn.distplot((left + right) / 2.0, **kwargs)

    @LazyProperty
    def values(self):

        cur = self.parent_table.conn.cursor()

        cur.execute(self.select_all_query())

        return np.array([x[0] for x in cur.fetchall()])

    def _calculate_aggregate(self, aggregate):

        query = "select {}({}) from (\n{}\n)a".format(
            aggregate, self, self.select_all_query())

        cur = self.parent_table.conn.cursor()
        cur.execute(query)
        return cur.fetchone()[0]

    @LazyProperty
    def mean(self):

        return self._calculate_aggregate("avg")

    @LazyProperty
    def max(self):

        return self._calculate_aggregate("max")

    @LazyProperty
    def min(self):

        return self._calculate_aggregate("min")

    @LazyProperty
    def size(self):

        return self.parent_table.count

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<{} '{}'>".format(self.__class__, self.name)

    def __eq__(self, other):

        if not isinstance(other, Column):
            return False

        return self.name == other.name and self.parent_table == other.parent_table

    def __ne__(self, other):

        return not self.__eq__(other)
