from collections import defaultdict

import pandas as pd
import six
from lazy_property import LazyProperty

from .. import numeric_datatypes, _pretty_print
from ..column.base import Column
from ..exception import TableDoesNotExistError, NoSuchColumnError


class Table(object):
    """
    This class is used for representing table metadata.

    :ivar pg_utils.connection.Connection conn: A connection to be used by this table.
    :ivar str schema: The name of the schema in which this table lies.
    :ivar str table_name: The name of the given table within the above schema.
    :ivar tuple[str] _column_names: A list of column names for the table, as found in the database.
    :ivar tuple[str] numeric_columns: A list of column names corresponding
    to the _column_names in the table that have some kind of number datatype (``int``, ``float8``, ``numeric``, etc).
    :ivar dict[str, str] column_data_types: A dictionary giving the data type of each column (given by the column names
    as found in the ``_column_names`` attribute above).
    """

    def __init__(self, conn, schema, table_name,
                 columns=None, all_columns=None, all_column_data_types=None,
                 check_existence=True, debug=False):
        """

        :param pg_utils.connection.Connection conn:
        :param str schema: The name of the schema in which this table lies.
        :param str table_name: The name of the given table within the above schema.
        :param str|list[str]|tuple[str] columns: An iterable
        :param all_columns:
        :param all_column_data_types:
        :param check_existence:
        :param debug:
        :return:
        """

        self.conn = conn
        self._schema = schema
        self._table_name = table_name
        self._column_names = columns
        self._all_column_names = all_columns
        self.all_column_data_types = all_column_data_types
        self.check_existence = check_existence
        self.debug = debug

        self._validate()

        self._process_columns()

        for col in self.columns:
            setattr(self, col.name, col)

    def _validate(self):

        if self.check_existence and not Table.exists(self.conn, self.schema, self.table_name):
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
        cur.execute("drop table if exists {}.{} cascade;".format(schema, table_name))
        conn.commit()
        cur.execute(create_stmt)
        conn.commit()

        if not kwargs.get("check_existence"):
            kwargs["check_existence"] = False

        return cls(conn, schema, table_name, *args, **kwargs)

    @classmethod
    def from_table(cls, table, *args, **kwargs):
        kwargs["check_existence"] = False

        if "columns" in kwargs and isinstance(kwargs["columns"], six.string_types):
            col = kwargs["columns"]
            del kwargs["columns"]

            parent = Table.from_table(table, *args, **kwargs)

            return Column(col, parent)

        return cls(table.conn, table.schema, table.table_name, *args, **kwargs)

    def select_all_query(self):

        return "select {} from {}".format(",".join(self._column_names), self)

    @LazyProperty
    def count(self):
        """Returns the number of rows in the corresponding database table."""
        cur = self.conn.cursor()

        cur.execute("select count(1) from {}".format(self))

        return cur.fetchone()[0]

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

        result = pd.read_sql(query, self.conn, **kwargs)

        if len(self._column_names) == 1:
            result = result[self._column_names[0]]

        return result

    @LazyProperty
    def shape(self):
        """
        As in the property of Pandas DataFrames by the same name, this gives a tuple showing the dimensions of the table: ``(number of rows, number of _column_names)``
        """

        return self.count, len(self._column_names)

    @LazyProperty
    def dtypes(self):

        return pd.Series([c.dtype for c in self.columns], index=self._column_names)

    def get_dtype_counts(self):

        counts = defaultdict(int)

        dtypes = self.dtypes

        for dt in dtypes:
            counts[dt] += 1

        return pd.Series(
                [counts[dt] for dt in sorted(list(counts.keys()))],
                index=sorted(list(counts.keys()))
        )

    def describe(self, columns=None, percentiles=None, type_="continuous"):

        if columns is None:
            columns = self.numeric_columns

        # TODO: Make these manipulations of percentiles a bit more DRY (this is also in column.base)
        if percentiles is None:
            percentiles = [0.25, 0.5, 0.75]
        elif not bool(percentiles):
            percentiles = []

        if not isinstance(percentiles, (list, tuple)):
            percentiles = [percentiles]

        column_subqueries = [
            q for q in
            [
                col._get_describe_query(percentiles=percentiles, type_=type_)
                for col in [self._get_column_by_name(col) for col in columns]
                ]
            if q
            ]

        query = "select * from (\n{}\n)a".format("\nunion all\n".join(column_subqueries))

        if self.debug:
            _pretty_print(query)

        cur = self.conn.cursor()
        cur.execute(query)

        result = {}

        index = ["count", "mean", "std_dev", "minimum"] + \
                ["{}%".format(int(100 * p)) for p in percentiles] + \
                ["maximum"]

        for row in cur.fetchall():
            result[row[0]] = row[1:]

        result = pd.DataFrame(result, columns=columns, index=index)

        return result

    @LazyProperty
    def _all_column_metadata(self):
        return pd.read_sql("""
        select column_name,
            case
                when lower(data_type) = 'array' then column_alias||'[]'
                else data_type
            end as data_type
        from(
            select
            column_name,
            data_type,
            translate(udt_name, '0123456789_', '') as column_alias,
            ordinal_position
            from information_schema.columns
            where table_schema = '{}'
            and table_name = '{}'
        )a
        order by ordinal_position;""".format(self.schema, self.table_name), self.conn)

    def _process_columns(self):

        self._all_column_names = self._all_column_names or [x for x in self._all_column_metadata.column_name]
        self.all_column_data_types = self.all_column_data_types or {
            row["column_name"]: row["data_type"]
            for i, row in self._all_column_metadata.iterrows()
            }

        if self._column_names is None:
            self._column_names = tuple(x for x in self._all_column_names)
        elif isinstance(self._column_names, six.string_types):
            self._column_names = (self._column_names,)

        if [x for x in self._column_names if x not in self._all_column_names]:
            raise NoSuchColumnError(str([x for x in self._column_names if x not in self._all_column_names]))

        self.all_columns = tuple(
                Column(col, self) for col in self._all_column_names
        )

        self.columns = tuple(
                col for col in self.all_columns if col.name in self._column_names
        )

    def _get_column_by_name(self, name):

        return self.columns[self._column_names.index(name)]

    @LazyProperty
    def all_numeric_columns(self):
        return [x for x in self._all_column_names
                if self.all_column_data_types[x] in numeric_datatypes]

    @LazyProperty
    def numeric_columns(self):
        return tuple(
                col for col in self._column_names
                if self.column_data_types[col] in numeric_datatypes
        )

    @LazyProperty
    def all_numeric_array_columns(self):
        return [col for col in self._all_column_names
                if self.all_column_data_types[col][-2:] == "[]"
                and self.all_column_data_types[col][:-2] in numeric_datatypes]

    @LazyProperty
    def numeric_array_columns(self):
        return tuple(
                col for col in self.all_numeric_array_columns if col in self._column_names
        )

    @LazyProperty
    def column_data_types(self):
        return {
            col: data_type for col, data_type in self.all_column_data_types.items()
            if col in self._column_names
            }

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

        :param pg_utils.connection.Connection conn: A connection to the database.
        :param str schema: The name of the schema.
        :param str table_name: The name of the table.
        :return: Whether or not the table exists.
        :rtype: bool
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
            if column_list in self._column_names:
                result = getattr(self, column_list)
            else:
                raise KeyError("Column '{}' not found in table '{}'".format(
                        column_list, self
                ))
        else:

            result = Table.from_table(self, columns=[x for x in column_list])

        return result

    def __str__(self):
        """
        The string representation of a ``Table`` object is the
        fully-qualified table name, as represented by the
        ``name`` property above.
        """
        return self.name

    def __repr__(self):
        return "<Table '{}'>".format(self.name)

    def __del__(self):

        for col in self._column_names:
            del col
