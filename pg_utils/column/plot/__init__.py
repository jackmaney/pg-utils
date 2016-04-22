from functools import wraps

import pandas as pd


def plot_dispatch(f):
    @wraps(f)
    def wrapper(self, **kwargs):
        return self(kind=f.__name__, **kwargs)

    wrapper.__doc__ = """Used to mimic the `pandas Series plotting API <http://pandas.pydata.org/pandas-docs/stable/api.html#plotting>`_ for {} plots. See `the documentation of pandas.Series.plot <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.plot.html#pandas.Series.plot>`_ for details on keyword arguments.""".format(
        f.__name__.capitalize() if f.__name__ != "hist" else "Histogram")

    return wrapper


class Plotter(object):
    def __init__(self, column):
        self.column = column

    @plot_dispatch
    def area(self, **kwargs): pass

    @plot_dispatch
    def bar(self, **kwargs): pass

    @plot_dispatch
    def barh(self, **kwargs): pass

    @plot_dispatch
    def box(self, **kwargs): pass

    @plot_dispatch
    def density(self, **kwargs): pass

    @plot_dispatch
    def hist(self, **kwargs): pass

    @plot_dispatch
    def kde(self, **kwargs): pass

    @plot_dispatch
    def line(self, **kwargs): pass

    @plot_dispatch
    def pie(self, **kwargs): pass

    def __call__(self, kind="line", **kwargs):

        data = pd.Series(self.column.head("all"))

        return data.plot(kind=kind, **kwargs)
