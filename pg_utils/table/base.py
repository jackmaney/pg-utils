from ..exception import TableDoesNotExistError

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
        return ".".join([self.schema, self.table_name])

    @staticmethod
    def exists(conn, schema, table_name):
        cur = conn.cursor()
        cur.execute("""
          select count(1) from information_schema.tables
          where table_schema='{}' and table_name='{}'
          """.format(schema, table_name)
                    )

        return bool(cur.fetchone()[0])
