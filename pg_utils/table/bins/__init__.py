import math
import six

from jinja2 import Environment, FileSystemLoader

from ... import template_dir
from ..base import Table

__all__ = ["bin_counts"]

_env = Environment(loader=FileSystemLoader(template_dir))
_bin_counts_template = _env.get_template("bin_counts.j2")



def _freedman_diaconis_num_bins(conn, schema, table_name, column):
    """
    https://en.wikipedia.org/wiki/Freedman%E2%80%93Diaconis_rule

    http://stats.stackexchange.com/questions/798/calculating-optimal-number-of-bins-in-a-histogram-for-n-where-n-ranges-from-30

    https://github.com/mwaskom/seaborn/blob/73f2fea2ecbaeb9b9254a3ae02523c0e564c82b6/seaborn/distributions.py#L23

    :param pg_utils.connection.Connection conn: A connection to the database.
    :param str schema: The name of the schema.
    :param str table_name: The name of the table.
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

def bin_counts(conn, schema, table_name, column, bins=None):
    """
    Returns bin counts for one or more column within a table. This can then be used for various kinds of plots (mostly bar charts and histograms).

    :param pg_utils.connection.Connection conn: A connection to the database.
    :param str schema: The name of the schema.
    :param str table_name: The name of the table.
    :param str column: The column for which bin counts will be computed.
    :param int|None bins: A positive integer specifying the number of bins or ``None``,
    in which case the Freedman-Diaconis rule will be used to determine the number of bins.
    :return: A list of triples (each represented as lists), where the first and second entries represent the bin boundaries, and the third represents the number of data points found in that bin.
    :rtype: list[list[float]]
    """

    if bins is not None and (not isinstance(bins, six.integer_types) or bins <= 0):
        raise ValueError("'bins' must be a positive integer or None!")

    t = Table(conn, schema, table_name)

    if column not in t.numeric_columns:
        raise ValueError("The column {} is not a numeric column of {}".format(column, t))

    global _desc
    _desc = t.describe(columns=[column], percentiles=[0.25, 0.75])[column]

    if bins is None:
        bins = min(_freedman_diaconis_num_bins(conn, schema, table_name, column), 50)

    cur = conn.cursor()

    sql = _bin_counts_template.render(
        bin_width=(_desc["maximum"] - _desc["minimum"]) / bins,
        bins=bins,
        table_name=t.name,
        column=column,
        minimum=_desc["minimum"],
        maximum=_desc["maximum"]
    )

    cur.execute(sql)

    return [row[1:] for row in cur.fetchall()]







