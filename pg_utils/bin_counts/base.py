import six
from . import freedman_diaconis
from .. import template_dir
from jinja2 import Environment, FileSystemLoader

_env = Environment(loader=FileSystemLoader(template_dir))
_bin_counts_template = _env.get_template("bin_counts.j2")

def counts(table, column, bins=None):
    """
    Retrieves the counts of values in a given column for a given number of bin_counts.

    :param pg_utils.table.Table table: The table containing the column which is to be binned.
    :param str column: The name of a column which you want to bin_counts.
    :param int|None bins: The number of bin_counts that you want. If set to ``None``,
     then the `Freedman-Diaconis rule <https://en.wikipedia.org/wiki/Freedman%E2%80%93Diaconis_rule>`_ will be used.
    :return: A list of lists. Each sublist represents the count of items in a particular bin_counts and is of the form ``[left_endpoint, right_endpoint, bin_count]``.
    :rtype: list[list[float]]
    """

    if bins is not None and (not isinstance(bins, six.integer_types) or bins <= 0):
        raise ValueError("'bin_counts' must be a positive integer or None!")

    if column not in table.numeric_columns:
        raise ValueError("The column {} is not a numeric column of {}".format(column, table))

    desc = table.describe(columns=[column], percentiles=[0.25, 0.75])[column]

    if bins is None:
        bins = min(freedman_diaconis.num_bins(table, column, desc=desc), 50)

    cur = table.conn.cursor()

    sql = _bin_counts_template.render(
        bin_width=(desc["maximum"] - desc["minimum"]) / bins,
        bins=bins,
        table_name=table.name,
        column=column,
        minimum=desc["minimum"],
        maximum=desc["maximum"]
    )

    cur.execute(sql)

    return [row[1:] for row in cur.fetchall()]
