import re

from ._version import __version__
import os

base_dir = os.path.realpath(os.path.dirname(__file__))
template_dir = os.path.join(base_dir, "templates")



numeric_datatypes = [
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


def _pretty_print(query):
    print("\n".join([line for line in re.split("\r?\n", query) if not re.match("^\s*$", line)]))
