import numpy as np
import pandas as pd
from lazy_property import LazyProperty

from . import _describe_template
from .plot import Plotter
from .. import bin_counts
from .. import numeric_datatypes, _pretty_print
from ..util import seaborn_required


class Column(object):
    """
    In Pandas, a column of a DataFrame is represented as a Series.

    Similarly, a column in a database table is represented by
    an object from this class.

    Note that the Series represented by these columns have the default index (ie non-negative, consecutive integers starting at zero). Thus, for the portion of the Pandas Series API mocked here, we need not worry about multilevel (hierarchical) indices.
    """

    def __init__(self, name, parent_table):
        """
        :param str name: The name of the column. Required.
        :param pg_utils.table.Table parent_table: The table to which this column belongs. Required.
        """

        self.parent_table = parent_table
        self.name = name
        self.is_numeric = parent_table._all_column_data_types[name] in numeric_datatypes

        self.plot = Plotter(self)

    def select_all_query(self):
        """
        Provides the SQL used when selecting everything from this column.

        :return: The SQL statement.
        :rtype: str
        """

        return "select {} from {}".format(self, self.parent_table)

    def sort_values(self, ascending=True, limit=None, **sql_kwargs):
        """
        Mimics the method `pandas.Series.sort_values <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.sort_values.html#pandas.Series.sort_values>`_.

        :param int|None limit: Either a positive integer for the number of rows to take or ``None`` to take all.
        :param bool ascending: Sort ascending vs descending.
        :param dict sql_kwargs: A dictionary of keyword arguments passed into `pandas.read_sql <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html>`_.
        :return: The resulting series.
        :rtype: pandas.Series
        """

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError("limit must be a positive integer or None (got {})".format(limit))

        sql = self.select_all_query() + " order by 1"

        if not ascending:
            sql += " desc"

        if limit is not None:
            sql += " limit {}".format(limit)

        return pd.read_sql(sql, self.parent_table.conn, **sql_kwargs)[self.name]

    def unique(self):
        """
        Returns an array of unique values in this column. Includes ``null`` (represented as ``None``).
        :return: The unique values.
        :rtype: np.array
        """

        cur = self.parent_table.conn.cursor()
        cur.execute("select distinct {} from {}".format(self, self.parent_table))
        return np.array([x[0] for x in cur.fetchall()])

    def hist(self, **kwargs):

        return self.plot.hist(**kwargs)

    def head(self, num_rows=10):
        """
        Fetches some values of this column.

        :param int|str num_rows: Either a positive integer number of values or the string `"all"` to fetch all values
        :return: A NumPy array of the values
        :rtype: np.array
        """

        if (isinstance(num_rows, int) and num_rows < 0) or \
                        num_rows != "all":
            raise ValueError("num_rows must be a positive integer or the string 'all'")

        query = self.select_all_query()

        if num_rows != "all":
            query += " limit {}".format(num_rows)

        cur = self.parent_table.conn.cursor()

        cur.execute(query)
        return np.array([x[0] for x in cur.fetchall()])

    @LazyProperty
    def is_unique(self):
        """
        Determines whether or not the values of this column are all unique (ie whether this column is a unique identifier for the table).
        :return: Whether or not this column contains unique values.
        :rtype: bool
        """

        cur = self.parent_table.conn.cursor()

        cur.execute("""select {}
          from {}
          group by 1 having count(1) > 1""".format(self, self.parent_table))

        return cur.fetchone() is None

    @LazyProperty
    def dtype(self):
        """
        The ``dtype`` of this column (represented as a string).

        :return: The ``dtype``.
        :rtype: str
        """

        return self.parent_table._all_column_data_types[self.name]

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
        """
        This mocks the method `pandas.Series.describe`, and provides
        a series with the same data (just calculated by the database).

        :param None|list[float] percentiles: A list of percentiles to evaluate (with numbers between 0 and 1). If not specified, quartiles (0.25, 0.5, 0.75) are used.
        :param str type_: Specifies whether the percentiles are to be taken as discrete or continuous. Must be one of `"discrete"` or `"continuous"`.
        :return: A series returning the description of the column, in the same format as ``pandas.Series.describe``.
        :rtype: pandas.Series
        """

        if percentiles is None:
            percentiles = [0.25, 0.5, 0.75]

        cur = self.parent_table.conn.cursor()

        cur.execute(self._get_describe_query(percentiles=percentiles, type_=type_))

        index = ["count", "mean", "std_dev", "minimum"] + \
                ["{}%".format(int(100 * p)) for p in percentiles] + \
                ["maximum"]

        return pd.Series(cur.fetchone()[1:], index=index)

    @seaborn_required
    def distplot(self, bins=None, **kwargs):
        """
        Produces a ``distplot``. See `the seaborn docs <http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.distplot.html>`_ on ``distplot`` for more information.

        Note that this requires Seaborn in order to function.

        :param int|None bins: The number of bins to use. If unspecified, the `Freedman-Diaconis rule <https://en.wikipedia.org/wiki/Freedman%E2%80%93Diaconis_rule>`_ will be used to determine the number of bins.
        :param dict kwargs: A dictionary of options to pass on to `seaborn.distplot <http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.distplot.html>`_.
        """

        import seaborn

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
        """
        Mocks the method `pandas.Series.values`, returning a simple NumPy array
        consisting of the values of this column.

        :return: The NumPy array containing the values.
        :rtype: np.array
        """

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
        """
        Mocks the ``pandas.Series.mean`` method to give the mean of the values in this column.
        :return: The mean.
        :rtype: float
        """

        return self._calculate_aggregate("avg")

    @LazyProperty
    def max(self):
        """
        Mocks the ``pandas.Series.max`` method to give the maximum of the values in this column.
        :return: The maximum.
        :rtype: float
        """

        return self._calculate_aggregate("max")

    @LazyProperty
    def min(self):
        """
        Mocks the ``pandas.Series.min`` method to give the maximum of the values in this column.
        :return: The minimum.
        :rtype: float
        """

        return self._calculate_aggregate("min")

    @LazyProperty
    def size(self):
        """
        Mocks the ``pandas.Series.size`` property to give a count of the values in this column.
        :return: The count.
        :rtype: int
        """

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
