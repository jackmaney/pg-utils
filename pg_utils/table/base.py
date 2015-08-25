from ..exception import TableDoesNotExistError
import pandas as pd
import six

__all__ = ["Table"]

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

    def __init__(self, conn, schema, table_name):

        self.conn = conn
        self._schema = schema
        self._table_name = table_name

        if not Table.exists(conn, schema, table_name):
            raise TableDoesNotExistError("Table {}.{} does not exist".format(
                schema, table_name
            ))

        self._num_rows = None

        self._get_column_data()

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

        query = "select * from {}".format(self)

        if num_rows != "all":
            query += " limit {}".format(num_rows)

        return pd.read_sql(query, self.conn, **kwargs)

    def count(self):
        """Returns the number of rows in the corresponding database table."""
        cur = self.conn.cursor()

        cur.execute("select count(1) from {}".format(self.name))

        return cur.fetchone()[0]

    def _get_column_data(self):

        cur = self.conn.cursor()

        cur.execute("""
            select column_name, data_type,
            translate(udt_name, '0123456789_', '') as column_alias
            from information_schema.columns
            where table_schema='{}' and table_name='{}'
            order by ordinal_position
        """.format(self.schema, self.table_name))

        columns = []
        column_data_types = {}
        numeric_array_columns = []

        for row in cur.fetchall():
            columns.append(row[0])
            if row[1].lower() == "array":
                data_type = "{}[]".format(row[2])
                if row[2].lower() in _numeric_datatypes:
                    numeric_array_columns.append(row[0])
            else:
                data_type = row[1]

            column_data_types[row[0]] = data_type

        self.column_data_types = column_data_types
        self.columns = tuple(columns)

        self.numeric_columns = tuple(
            x for x in self.columns
            if self.column_data_types[x]
            in _numeric_datatypes
        )

        self.numeric_array_columns = tuple(numeric_array_columns)

    @property
    def num_rows(self):
        """
        Returns the number of rows of the table.

        .. note::

            This is a lazy attribute, only calling the ``count`` method th e first time it is used.


        """

        if self._num_rows is None:
            self._num_rows = self.count()

        return self._num_rows

    @num_rows.setter
    def num_rows(self, value):
        self._num_rows = value

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

    def __str__(self):
        """
        The string representation of a ``Table`` object is the
        fully-qualified table name, as represented by the
        ``name`` property above.
        """
        return self.name

    def __repr__(self):
        return "<Table '{}'>".format(self.name)
