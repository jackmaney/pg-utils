from .. import template_dir
from jinja2 import Environment, FileSystemLoader

_env = Environment(loader=FileSystemLoader(template_dir))
_describe_template = _env.get_template("describe_column.j2")
_bin_counts_template = _env.get_template("bin_counts.j2")

from .base import Column
