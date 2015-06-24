import setuptools
from pg_utils._version import __version__

try:
    with open("README.rst") as f:
        long_description = f.read()
except IOError:
    long_description = ""

requirements = [
    "psycopg2",
    "click",
    "pandas"
]

setuptools.setup(
    name="pg-utils",
    author="Jack Maney",
    version=__version__,
    install_requires=requirements,
    packages=setuptools.find_packages()
)
