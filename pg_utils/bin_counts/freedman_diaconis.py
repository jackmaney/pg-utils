import math

__all__ = ["num_bins"]

def num_bins(table, column, desc=None):
    """
    https://en.wikipedia.org/wiki/Freedman%E2%80%93Diaconis_rule

    http://stats.stackexchange.com/questions/798/calculating-optimal-number-of-bin_counts-in-a-histogram-for-n-where-n-ranges-from-30

    https://github.com/mwaskom/seaborn/blob/73f2fea2ecbaeb9b9254a3ae02523c0e564c82b6/seaborn/distributions.py#L23

    NOTE: This function assumes that the global variable ``desc`` has been set to a Pandas Series
    containing the description information of ``column``.

    :param pg_utils.table.Table table: The table containing the column which is to be binned.
    :param str column: A column name for which bin_counts counts will be computed.
    :param pd.Series desc: An optional series produced by the ``describe`` method of ``table`` containing the 25th and 75th percentiles (so that pre-computed values can be optionally passed in). If not specified, then ``table.describe`` will be called.
    :return: The number of bin_counts to use.
    :rtype: int
    """

    desc = desc if desc is not None else table.describe(columns=[column], percentiles=[0.25, 0.75])[column]

    if desc["count"] == 0:
        raise ValueError("Cannot compute Freedman-Diaconis bin_counts count for a count of 0")

    h = 2 * (desc["75%"] - desc["25%"]) / (desc["count"] ** (1.0 / 3.0))

    if h == 0:
        return math.ceil(math.sqrt(desc["count"]))
    else:
        return math.ceil((desc["maximum"] - desc["minimum"]) / h)
