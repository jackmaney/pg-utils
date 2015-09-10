from ._version import __version__
import os

base_dir = os.path.realpath(os.path.dirname(__file__))
template_dir = os.path.join(base_dir, "templates")

from . import table, connection
