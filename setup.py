import setuptools
from pg_utils import __version__

try:
    with open("README.rst") as f:
        long_description = f.read()
except IOError:
    long_description = ""

try:
    with open("requirements.txt") as f:
        requirements = [x.strip() for x in f.read().splitlines() if x.strip()]
except OSError:
    requirements = []

setuptools.setup(
    name="pg-utils",
    author="Jack Maney",
    author_email="jackmaney@gmail.com",
    version=__version__,
    install_requires=requirements,
    extras_require={
        "graphics": ["seaborn"]
    },
    packages=setuptools.find_packages(),
    url="https://github.com/jackmaney/pg-utils",
    license="MIT",
    description="Utility libraries for working with PostgreSQL",
    include_package_data=True
)
