import warnings

from jinja2 import Environment, FileSystemLoader

from .. import template_dir

warnings.filterwarnings("ignore", message="axes.color_cycle is deprecated")

__all__ = []

_env = Environment(loader=FileSystemLoader(template_dir))
_describe_template = _env.get_template("describe.j2")
_bin_counts_template = _env.get_template("bin_counts.j2")


