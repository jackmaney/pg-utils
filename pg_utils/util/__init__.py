"""
This module just contains utility functions that don't directly fit anywhere else. You shouldn't need to tinker with these.
"""
import inspect
from functools import wraps
from getpass import getuser

from six import PY2

from ..connection import Connection


# noinspection PyDeprecation
def position_of_positional_arg(arg_name, fcn):
    """
    Finds the index of a function's named positional arguments that has a specific name. For example:

    ::

        In [1]: def foo(x, y, z, w=1, q=None): pass

        In [2]: _position_of_positional_arg("y", foo)
        Out[2]: 1

        In [3]: _position_of_positional_arg("z", foo)
        Out[3]: 2

        In [4]: _position_of_positional_arg("w", foo)
        ---------------------------------------------------------------------------
        ValueError                                Traceback (most recent call last)

        <...snip...>

        ValueError: Argument 'w' not found as a named positional argument of function foo


    :param str arg_name: The name of the parameter to search for.
    :param fcn: A function whose parameters we will search.
    :return: The index (if it exists).
    :rtype: int
    :raises: ValueError: If ``arg_name`` isn't the name of any named positional argument of ``fcn``.
    """

    if PY2:
        args, _, _, defaults = inspect.getargspec(fcn)
        defaults = defaults or []
        if arg_name in args[:len(args) - len(defaults) - 1]:
            return args[:len(args) - len(defaults) - 1].index(arg_name)
        else:
            raise ValueError(
                "Argument '{}' not found as a named positional argument of function {}".format(arg_name, fcn.__name__))
    else:
        sig = inspect.signature(fcn)

        param = sig.parameters.get(arg_name)

        if param is None or param.default is not param.empty:
            raise ValueError(
                "Argument '{}' not found as a named positional argument of function {}".format(arg_name, fcn.__name__))

        return [p.name for p in sig.parameters.values() if p.default is param.empty].index(arg_name)


def process_schema_and_conn(f):
    """
    This decorator does the following (which is needed a few times in the ``Table`` class):

    * Standardizes the ``table_name`` and ``schema`` parameters (the first of which must be positional and named, and the latter of which must be a keyword parameter). In particular, if ``table_name == "foo.bar"``, then ``schema`` is replaced with ``"foo"`` and ``table_name`` with ``"bar"``. If the table name is not qualified with a schema, then both ``schema`` and ``table_name`` are left alone.

    * If no ``conn`` keyword argument is given (or if it's ``None``), then that keyword argument is replaced with a fresh :class:`Connection` object.

    :raises ValueError: if mismatching schemas are found in both ``schema`` and ``table_name``

    :raises ValueError: if we can't parse the table name (ie more than one occurrence of ``"."``).
    """

    @wraps(f)
    def decorator(*args, **kwargs):

        args = list(args)  # args is given as a tuple, and we may need to alter it...

        kwargs.setdefault("conn", Connection())

        schema = kwargs.get("schema")

        idx = position_of_positional_arg("table_name", f)
        table_name = args[idx]

        if len(table_name.split(".")) == 2:
            if schema is not None and schema != table_name.split(".")[0]:
                raise ValueError(
                    "Schema '{}' does not match what is specified by the table_name of '{}'".format(schema, table_name))

            schema, table_name = table_name.split(".")

        elif len(table_name.split(".")) == 1:

            schema = schema or getuser()

        else:

            raise ValueError("Unable to parse table_name of '{}'".format(table_name))

        kwargs.setdefault("schema", schema)
        args[idx] = table_name

        return f(*args, **kwargs)

    return decorator


def seaborn_required(f):
    """
    This decorator makes sure that seaborn is imported before the function is run.
    """

    @wraps(f)
    def decorator(*args, **kwargs):
        try:
            import seaborn
            import numpy as np
        except ImportError as e:
            raise ImportError(
                "You do not have seaborn installed (or there was an issue importing it). Please install seaborn (or pg-utils[graphics])")

        return f(*args, **kwargs)

    return decorator
