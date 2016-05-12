import tempfile
from collections import defaultdict

import pandas as pd
import six
from lazy_property import LazyProperty

from .. import numeric_datatypes, _pretty_print
from ..column.base import Column
from ..connection import Connection
from ..exception import TableDoesNotExistError, NoSuchColumnError
from ..util import process_schema_and_conn, seaborn_required


class Table(object):
    """
    This class is used for representing table metadata.

    :ivar pg_utils.connection.Connection conn: A connection to be used by this table.
    :ivar str name: The fully-qualified name of this table.
    :ivar tuple[str] column_names: A list of column names for the table, as found in the database.
    :ivar tuple[Column] columns: A tuple of :class:`Column` objects.
    :ivar tuple[str] numeric_columns: A list of column names corresponding to the column_names in the table that have some kind of number datatype (``int``, ``float8``, ``numeric``, etc).
    """

    @process_schema_and_conn
    def __init__(self, table_name, schema=None, conn=None, columns=None, check_existence=True, debug=False):
        """

        :param str table_name: The name of the table in the database. If it's qualified with a schema, then leave the ``schema`` argument alone.
        :param None|str schema: The name of the schema in which this table lies. If unspecified and the value of ``table_name`` doesn't include a schema, then the (OS-specified) username of the given user is taken to be the schema.
        :param None|pg_utils.connection.Connection conn: A connection object that's used to fetch data and metadata. If not specified, a new connection is made with default arguments provided for username, password, etc.
        :param str|list[str]|tuple[str] columns: An iterable of specified column names. It's used by the ``__getitem__`` magic method, so you shouldn't need to fiddle with this.
        :param bool check_existence: If enabled, an extra check is made to ensure that the table referenced by this object actually exists in the database.
        :param bool debug: Enable to get some extra logging that's useful for debugging stuff.
        """

        self._table_name = table_name
        self._schema = schema
        self.conn = conn

        self.column_names = columns
        self._all_column_names = None
        self._all_column_data_types = None
        self.check_existence = check_existence
        self.debug = debug

        self._validate()

        self._process_columns()

        for col in self.columns:
            setattr(self, col.name, col)

    def _validate(self):

        if self.check_existence and not Table.exists(self.table_name, conn=self.conn, schema=self.schema):
            raise TableDoesNotExistError("Table {} does not exist".format(self))

    @classmethod
    @process_schema_and_conn
    def create(cls, table_name, create_stmt, conn=None, schema=None,
               *args, **kwargs):
        """
        This is the constructor that's easiest to use when creating a new table.

        :param str table_name: As mentioned above.
        :param str create_stmt: A string of SQL (presumably including a "CREATE TABLE" statement for the corresponding database table) that will be executed before ``__init__`` is run.

        .. note::

            The statement ``drop table if exists schema.table_name;`` is
            executed **before** the SQL in ``create_stmt`` is executed.

        :param None|pg_utils.connection.Connection conn: A ``Connection`` object to use for creating the table. If not specified, a new connection will be created with no arguments. Look at the docs for the Connection object for more information.
        :param None|str schema: A specified schema (optional).

        :param args: Other positional arguments to pass to the initializer.
        :param kwargs: Other keyword arguments to pass to the initializer.
        :return: The corresponding ``Table`` object *after* the ``create_stmt`` is executed.
        """

        update_kwargs = {"check_existence": False, "conn": conn, "schema": schema}

        cur = conn.cursor()

        drop_stmt = "drop table if exists {} cascade;".format(table_name)

        if schema is not None:
            drop_stmt = "drop table if exists {}.{} cascade;".format(schema, table_name)

        cur.execute(drop_stmt)
        conn.commit()
        cur.execute(create_stmt)
        conn.commit()

        kwargs.update(update_kwargs)

        return cls(table_name, *args, **kwargs)

    @classmethod
    def from_table(cls, table, *args, **kwargs):
        """
        This class method constructs a table from a given table. Used to give a fresh ``Table`` object with different columns, but all other parameters the same as the given table.

        If the ``columns`` attribute only specifies one column, then a :class:`Column` object will be returned.
        :param Table table: The table object from which the output will be created.
        :param list args: Any positional arguments (if any).
        :param dict kwargs: Any keyword arguments to pass along (if any).
        :return: Either a fresh ``Table`` or :class:`Column`, depending on whether the ``columns`` parameter is restricted to just a single column.
        :rtype: Column|Table
        """
        kwargs["check_existence"] = False

        kwargs.update({attr: getattr(table, attr)
                       for attr in ["conn", "schema", "debug"]})
        kwargs.setdefault("columns", table.column_names)

        if "columns" in kwargs and isinstance(kwargs["columns"], six.string_types):
            col = kwargs["columns"]
            del kwargs["columns"]

            parent = Table.from_table(table, *args, **kwargs)

            result = Column(col, parent)

        else:

            result = cls(table.table_name, *args, **kwargs)

        return result

    def select_all_query(self):

        return "select {} from {}".format(",".join(self.column_names), self)

    @LazyProperty
    def count(self):
        """Returns the number of rows in the corresponding database table."""
        cur = self.conn.cursor()

        cur.execute("select count(1) from {}".format(self))

        return cur.fetchone()[0]

    def head(self, num_rows=10, **read_sql_kwargs):
        """
        Returns some of the rows, returning a corresponding Pandas DataFrame.

        :param int|str num_rows: The number of rows to fetch, or ``"all"`` to fetch all of the rows.
        :param dict read_sql_kwargs: Any other keyword arguments that you'd like to pass into ``pandas.read_sql`` (as documented `here <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html>`_).
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

        result = pd.read_sql(query, self.conn, **read_sql_kwargs)

        if len(self.column_names) == 1:
            result = result[self.column_names[0]]

        return result

    @LazyProperty
    def shape(self):
        """
        As in the property of Pandas DataFrames by the same name, this gives a tuple showing the dimensions of the table: ``(number of rows, number of columns)``
        """

        return self.count, len(self.column_names)

    @LazyProperty
    def dtypes(self):
        """
        Mimics the `pandas.DataFrame.dtypes` property, giving a Series of dtypes (given as strings) corresponding to each column.
        :return: The Series of dtypes.
        :rtype: pd.Series
        """

        return pd.Series([c.dtype for c in self.columns], index=self.column_names)

    def get_dtype_counts(self):

        counts = defaultdict(int)

        dtypes = self.dtypes

        for dt in dtypes:
            counts[dt] += 1

        return pd.Series(
            [counts[dt] for dt in sorted(list(counts.keys()))],
            index=sorted(list(counts.keys()))
        )

    def insert(self, row, columns=None):
        """
        Inserts a single tuple into the table.

        :param list|pandas.Series row: A list or Series of items to insert. If a list, its length must match up with the number of columns that we'll insert. If it's a series, the column names must be contained within the index.
        :param None|list[str]|tuple[str] columns: An iterable of column names to use, that must be contained within this table. If not specified, all of the columns are taken.
        :return: Returns a boolean indicating success.
        :rtype: bool
        """

        if columns is None:

            columns = self.column_names

        elif any([c for c in columns if c not in self.column_names]):
            raise ValueError("The following columns are not in table {}: {}".format(
                self, ",".join([str(c) for c in columns if c not in self.column_names])
            ))

        columns = list(columns)

        if isinstance(row, pd.Series):
            if any([x for x in row.index if x not in columns]):
                raise ValueError(
                    "The following index elements are not specified columns: {}".format(
                        ",".join([str(x) for x in row.index if x not in columns])
                    ))
            columns = [c for c in columns if c in row.index]

        if len(columns) != len(row):
            raise ValueError(
                "Length of row to be inserted is not the same as the number of columns selected ({} vs {})".format(
                    len(row), len(columns)))

        stmt = """insert into {} ({}) values ({});""".format(
            self, ", ".join(columns), ", ".join(["%s"] * len(columns))
        )

        cur = self.conn.cursor()

        cur.execute(stmt, tuple(row))

        return bool(cur.rowcount)

    def insert_csv(self, file_name, columns=None, header=True, sep=",", null="", size=8192):
        """
        A wrapper around the `copy_expert <http://initd.org/psycopg/docs/cursor.html#cursor.copy_expert>`_ method of the psycopg2 cursor class to do a bulk insert into the table.

        :param str file_name: The name of the CSV file.
        :param None|list[str]|tuple[str] columns: An iterable of column names to use, that must be contained within this table. If not specified, all of the columns are taken.
        :param bool header: Indicates whether or not the file has a header.
        :param str sep: The separator character.
        :param str null: The string used to indicate null values.
        :param int size: The size of the buffer that ``psycopg2.cursor.copy_expert`` uses.
        """

        column_str = "" if columns is None else " ({})".format(",".join([str(x) for x in columns]))

        cmd = "copy {}{} from stdin delimiter '{}' null '{}' csv".format(self,
                                                                         column_str,
                                                                         sep, null)
        if header:
            cmd += " header"
        else:
            cmd += " no header"

        with open(file_name) as f:
            cur = self.conn.cursor()
            cur.copy_expert(sql=cmd, file=f, size=size)
            self.conn.commit()
            cur.close()

    def insert_dataframe(self, data_frame, encoding="utf8", **csv_kwargs):
        """
        Does a bulk insert of a given pandas DataFrame, writing it to a (temp) CSV file, and then importing it.

        :param pd.DataFrame data_frame: The DataFrame that is to be inserted into this table.
        :param str encoding: The encoding of the CSV file.
        :param csv_kwargs: Other keyword arguments that are passed to the ``insert_csv`` method.
        """

        with tempfile.NamedTemporaryFile(mode="w", encoding=encoding) as f:
            data_frame.to_csv(f, index=False, **csv_kwargs)
            f.seek(0)
            kwargs = {"columns": csv_kwargs.get("columns"),
                      "header": csv_kwargs.get("header", True),
                      "sep": csv_kwargs.get("sep", ","),
                      "null": csv_kwargs.get("na_rep", "")}
            self.insert_csv(f.name, **kwargs)

    def sort_values(self, by, ascending=True, **sql_kwargs):
        """
        Mimicks the `pandas.DataFrame.sort_values method <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.sort_values.html#pandas.DataFrame.sort_values>`_.

        :param str|list[str] by: A string or list of strings representing one or more column names by which to sort.
        :param bool|list[bool] ascending: Whether to sort ascending or descending. This must match the number of columns by which we're sorting, although if it's just a single value, it'll be used for all columns.
        :param dict sql_kwargs: A dictionary of keyword arguments passed into `pandas.read_sql <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html>`_.
        :return: Values of the sorted DataFrame.
        :rtype: pandas.DataFrame
        """

        sql = self.select_all_query()

        if isinstance(by, str):
            by = [by]

        if not set(by) <= set(self.column_names):
            raise ValueError("Column names '{}' not in table {}".format(
                ",".join(list(set(by) - set(self.column_names))), self
            ))

        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)

        if len(by) != len(ascending):
            raise ValueError("Mismatch between columns to sort by ({}), and ascending list ({})".format(by, ascending))

        pairs = [[by[i], "" if ascending[i] else " desc"] for i in list(range(len(by)))]

        sql += " order by " + ", ".join(["".join(p) for p in pairs])

        return pd.read_sql(sql, self.conn, **sql_kwargs)

    def describe(self, columns=None, percentiles=None, type_="continuous"):
        """
        Mimics the ``pandas.DataFrame.describe`` method, getting basic statistics of each numeric column.

        :param None|list[str] columns: A list of column names to which the description should be restricted. If not specified, then all numeric columns will be included.
        :param list[float]|None percentiles: A list of percentiles (given as numbers between 0 and 1) to compute. If not specified, quartiles will be used (ie 0.25, 0.5, 0.75).
        :param str type_: Specifies whether the percentiles are to be taken as discrete or continuous. Must be one of `"discrete"` or `"continuous"`.
        :return: A series representing the statistical description for each column. The format is the same as the output of ``pandas.DataFrame.describe``.
        :rtype: pd.DataFrame
        """

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

    @seaborn_required
    def pairplot(self, **kwargs):
        """Yields a Seaborn pairplot for all of the columns of this table that are of a numeric datatype.

        :param dict kwargs: Optional keyword arguments to pass into `seaborn.pairplot <https://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.pairplot.html#seaborn.pairplot>`_.
        :return: The grid of plots.
        """

        import seaborn
        return seaborn.pairplot(self[self.numeric_columns].head("all"), **kwargs)

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

        self._all_column_names = [x for x in self._all_column_metadata.column_name]
        self._all_column_data_types = {
            row["column_name"]: row["data_type"]
            for i, row in self._all_column_metadata.iterrows()
            }

        if self.column_names is None:
            self.column_names = tuple(x for x in self._all_column_names)
        elif isinstance(self.column_names, six.string_types):
            self.column_names = (self.column_names,)

        if [x for x in self.column_names if x not in self._all_column_names]:
            raise NoSuchColumnError(", ".join([str(x) for x in self.column_names if x not in self._all_column_names]))

        self.all_columns = tuple(
            Column(col, self) for col in self._all_column_names
        )

        self.columns = tuple(
            col for col in self.all_columns if col.name in self.column_names
        )

    def _get_column_by_name(self, name):

        return self.columns[self.column_names.index(name)]

    @LazyProperty
    def _all_numeric_columns(self):
        return [x for x in self._all_column_names
                if self._all_column_data_types[x] in numeric_datatypes]

    @LazyProperty
    def numeric_columns(self):
        """
        A tuple of names belonging to columns that have a numeric datatype.
        """
        return tuple(
            col for col in self.column_names
            if self.column_data_types[col] in numeric_datatypes
        )

    @LazyProperty
    def _all_numeric_array_columns(self):
        return [col for col in self._all_column_names
                if self._all_column_data_types[col][-2:] == "[]"
                and self._all_column_data_types[col][:-2] in numeric_datatypes]

    @LazyProperty
    def numeric_array_columns(self):
        """
            A tuple of names belonging to columns that are an array of a numeric datatype (eg ``int[]``, ``double precision[]``, etc).
        """
        return tuple(
            col for col in self._all_numeric_array_columns if col in self.column_names
        )

    @LazyProperty
    def column_data_types(self):
        """
        A dictionary mapping column names to their corresponding datatypes.
        """
        return {
            col: data_type for col, data_type in self._all_column_data_types.items()
            if col in self.column_names
            }

    @property
    def schema(self):
        """
        The name of the schema.
        """
        return self._schema

    @property
    def table_name(self):
        """
        The name of the table (without the schema included).
        """
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
        self.conn.commit()
        del self

    @staticmethod
    @process_schema_and_conn
    def exists(table_name, schema=None, conn=None):
        """
        A static method that returns whether or not the given table exists.



        :param str table_name: The name of the table.
        :param None|str schema: The name of the schema (or current username if not provided).
        :param None|pg_utils.connection.Connection conn: A connection to the database. If not provided, a new connection is created with default arguments.
        :return: Whether or not the table exists.
        :rtype: bool
        """

        conn = conn or Connection()

        cur = conn.cursor()
        query = """
          select count(1) from information_schema.tables
          where table_schema='{}' and table_name='{}'
          """.format(schema, table_name)
        cur.execute(query)

        return bool(cur.fetchone()[0])

    def __getitem__(self, column_list):

        if isinstance(column_list, six.string_types):
            if column_list in self.column_names:
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
        The string representation of a ``Table`` object is the fully-qualified table name, as represented by the ``name`` property above.
        """
        return self.name

    def __repr__(self):
        return "<Table '{}'>".format(self.name)

    def __del__(self):

        for col in self.column_names:
            del col

        del self
