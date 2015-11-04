from .. import template_dir
from jinja2 import Environment, FileSystemLoader
from ..exception import TableDoesNotExistError
import math
import pandas as pd
import re
import six
from lazy_property import LazyProperty

import seaborn as sns

__all__ = ["Table"]

# TODO: Make this cleaner...
_desc = None # Holds a description of the column under consideration for binning. Used for DRY.

def _freedman_diaconis_num_bins(column):
    """
    https://en.wikipedia.org/wiki/Freedman%E2%80%93Diaconis_rule

    http://stats.stackexchange.com/questions/798/calculating-optimal-number-of-bins-in-a-histogram-for-n-where-n-ranges-from-30

    https://github.com/mwaskom/seaborn/blob/73f2fea2ecbaeb9b9254a3ae02523c0e564c82b6/seaborn/distributions.py#L23

    NOTE: This function assumes that the global variable ``_desc`` has been set to a Pandas Series
    containing the description information of ``column``.

    :param str column: A column name for which bin counts will be computed.
    :return: The number of bins to use.
    :rtype: int
    """


    if _desc["count"] == 0:
        raise ValueError("Cannot compute Freedman-Diaconis bin count for a count of 0")

    h = 2 * (_desc["75%"] - _desc["25%"]) / (_desc["count"] ** (1.0/3.0))

    if h == 0:
        return math.ceil(math.sqrt(_desc["count"]))
    else:
        return math.ceil((_desc["maximum"] - _desc["minimum"]) / h)





def _pretty_print(query):
    print("\n".join([line for line in re.split("\r?\n", query) if not re.match("^\s*$", line)]))

_numeric_datatypes = [
    "smallint",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "real",
    "double precision",
    "serial",
    "bigserial",
    "float"
]

_env = Environment(loader=FileSystemLoader(template_dir))
_describe_template = _env.get_template("describe.j2")
_bin_counts_template = _env.get_template("bin_counts.j2")

class Table(object):
    """
    This class is used for representing table metadata.

    :ivar pg_utils.connection.Connection conn: A connection to be used by this table.
    :ivar str schema: The name of the schema in which this table lies.
    :ivar str table_name: The name of the given table within the above schema.
    :ivar tuple[str] columns: A list of column names for the table, as found in the database.
    :ivar tuple[str] numeric_columns: A list of column names corresponding
    to the columns in the table that have some kind of number datatype (``int``, ``float8``, ``numeric``, etc).
    :ivar dict[str, str] column_data_types: A dictionary giving the data type of each column (given by the column names
    as found in the ``columns`` attribute above).
    """

    def __init__(self, conn, schema, table_name, columns=None, debug=False):

        self.conn = conn
        self._schema = schema
        self._table_name = table_name
        self.columns = columns

        self.debug = debug

        self._validate()

        self._get_column_data()

    def _validate(self):

        if not Table.exists(self.conn, self.schema, self.table_name):
            raise TableDoesNotExistError("Table {} does not exist".format(self))

    @classmethod
    def create(cls, conn, schema,
               table_name, create_stmt,
               *args, **kwargs):
        """
        This is the constructor that's easiest to use when creating a new table.

        :param pg_utils.connection.Connection conn: A ``Connection`` object to use for creating the table.
        :param str schema: As mentioned above.
        :param str table_name: As mentioned above.
        :param str create_stmt: A string of SQL (presumably including a "CREATE TABLE" statement for the corresponding
        database table) that will be executed before ``__init__`` is run.

        .. note::

            The statement ``drop table if exists schema.table_name;`` is
            executed **before** the SQL in ``create_stmt`` is executed.

        :param args: Other positional arguments to pass to the initializer.
        :param kwargs: Other keyword arguments to pass to the initializer.
        :return: The corresponding ``Table`` object *after* the ``create_stmt`` is executed.
        """
        cur = conn.cursor()
        drop_stmt = "drop table if exists {}.{} cascade;".format(schema, table_name)
        cur.execute(drop_stmt)
        conn.commit()
        cur.execute(create_stmt)
        conn.commit()

        return cls(conn, schema, table_name, *args, **kwargs)

    def select_all_query(self):

        return "select {} from {}".format(",".join(self.columns), self)


    def head(self, num_rows=10, **kwargs):
        """
        Returns some of the rows, returning a corresponding Pandas DataFrame.

        :param int|str num_rows: The number of rows to fetch, or ``"all"``
        to fetch all of the rows.
        :param dict kwargs: Any other keyword arguments that
        you'd like to pass into ``pandas.read_sql``
        (as documented `here <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html>`_).
        :return: The resulting data frame.
        :rtype: pandas.core.frame.DataFrame
        """

        if (not isinstance(num_rows, six.integer_types) or num_rows <= 0) and \
                        num_rows != "all":
            raise ValueError(
                "'num_rows': Expected a positive integer or 'all'")

        query = self.select_all_query()

        if num_rows != "all":
            query += " limit {}".format(num_rows)

        return pd.read_sql(query, self.conn, **kwargs)

    @LazyProperty
    def count(self):
        """Returns the number of rows in the corresponding database table."""
        cur = self.conn.cursor()

        cur.execute("select count(1) from {}".format(self))

        return cur.fetchone()[0]

    @LazyProperty
    def shape(self):
        """
        As in the property of Pandas DataFrames by the same name, this gives a tuple showing the dimensions of the table: ``(number of rows, number of columns)``
        """

        return self.count, len(self.columns)

    def describe(self, columns=None, percentiles=None, type_="continuous"):

        if columns is None:
            columns = self.numeric_columns
        elif any([x not in self.numeric_columns for x in columns]):
            raise ValueError("Not all columns in {} found in the numeric columns of {}".format(
                columns, self
            ))

        if percentiles is None:
            percentiles = [0.25, 0.5, 0.75]
        elif any([x < 0 or x > 1 for x in percentiles]):
            raise ValueError(
                "The `percentiles` attribute must be None or consist of numbers between 0 and 1 (got {})".format(
                    percentiles))

        percentiles = [float("{0:.2f}".format(p)) for p in percentiles if p > 0]

        percentiles = sorted(percentiles)

        if type_.lower() not in ["continuous", "discrete"]:
            raise ValueError("The 'type_' parameter must be 'continuous' or 'discrete'")

        suffix = "cont" if type_.lower() == "continuous" else "desc"

        query = _describe_template.render(table=self, columns=columns,
                                          percentiles=percentiles, suffix=suffix)

        if self.debug:
            _pretty_print(query)

        cur = self.conn.cursor()
        cur.execute(query)

        result = {}

        index = ["count", "mean", "std_dev", "minimum"] + \
                    ["{}%".format(int(100*p)) for p in percentiles] + \
                    ["maximum"]

        for row in cur.fetchall():
            result[row[0]] = row[1:]

        result = pd.DataFrame(result, columns=columns, index=index)

        return result


    def _get_column_data(self):

        cur = self.conn.cursor()

        cur.execute("""
            select column_name, data_type,
            translate(udt_name, '0123456789_', '') as column_alias
            from information_schema.columns
            where table_schema='{}' and table_name='{}'
            order by ordinal_position
        """.format(self.schema, self.table_name))

        all_columns = []
        columns = []
        column_data_types = {}
        numeric_array_columns = []

        for row in cur.fetchall():
            all_columns.append(row[0])

            if self.columns is None:
                columns.append(row[0])

            if self.columns is None or row[0] in self.columns:
                if row[1].lower() == "array":
                    data_type = "{}[]".format(row[2])
                    if row[2].lower() in _numeric_datatypes:
                        numeric_array_columns.append(row[0])
                else:
                    data_type = row[1]

                column_data_types[row[0]] = data_type

        self.column_data_types = column_data_types

        if self.columns is None:
            self.columns = tuple(columns)

        self.numeric_array_columns = tuple(numeric_array_columns)
        self.all_columns = all_columns

    @LazyProperty
    def numeric_columns(self):
        return tuple(
            x for x in self.columns
            if self.column_data_types[x]
            in _numeric_datatypes
        )

    @property
    def schema(self):
        return self._schema

    @property
    def table_name(self):
        return self._table_name

    @property
    def name(self):
        """
        The fully-qualified name of the table.
        """
        return ".".join([self.schema, self.table_name])

    def drop(self):
        """
        Drops the table and deletes this object (by calling ``del`` on it).
        """
        cur = self.conn.cursor()
        cur.execute("drop table {} cascade".format(self))
        del self


    @staticmethod
    def exists(conn, schema, table_name):
        """
        A static method that returns whether or not the given table exists.
        """
        cur = conn.cursor()
        cur.execute("""
          select count(1) from information_schema.tables
          where table_schema='{}' and table_name='{}'
          """.format(schema, table_name)
                    )

        return bool(cur.fetchone()[0])

    def __getitem__(self, column_list):

        if isinstance(column_list, six.string_types):
            column_list = [column_list]

        result = Table(self.conn, self.schema, self.table_name,
                       columns=column_list, debug=self.debug)

        return result

    def distplot(self, column, **kwargs):

        bc = self._bin_counts(column, bins=kwargs.get("bins"))

        data = []

        # For each point in a bin, we'll count it at the midpoint of the bin.
        for entry in bc:
            for i in range(entry[2]):
                data.append((entry[0] + entry[1]) / 2.0)

        sns.distplot(data, **kwargs)

    def _bin_counts(self, column, bins=None):

        if bins is not None and (not isinstance(bins, six.integer_types) or bins <= 0):
            raise ValueError("'bins' must be a positive integer or None!")

        if column not in self.numeric_columns:
            raise ValueError("The column {} is not a numeric column of {}".format(column, self))

        global _desc
        _desc = self.describe(columns=[column], percentiles=[0.25, 0.75])[column]

        if bins is None:

            bins = min(_freedman_diaconis_num_bins(column), 50)

        cur = self.conn.cursor()

        sql = _bin_counts_template.render(
            bin_width=(_desc["maximum"] - _desc["minimum"]) / bins,
            bins=bins,
            table_name=self.name,
            column=column,
            minimum=_desc["minimum"],
            maximum=_desc["maximum"]
        )

        cur.execute(sql)

        return [row[1:] for row in cur.fetchall()]

    def __str__(self):
        """
        The string representation of a ``Table`` object is the
        fully-qualified table name, as represented by the
        ``name`` property above.
        """
        return self.name

    def __repr__(self):
        return "<Table '{}'>".format(self.name)


