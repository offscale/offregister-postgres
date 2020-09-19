from setuptools import setup, find_packages
from os import path, listdir
from functools import partial
from ast import parse
from distutils.sysconfig import get_python_lib

if __name__ == "__main__":
    package_name = "offregister_postgres"

    f_for = partial(path.join, path.dirname(__file__), package_name)
    d_for = partial(path.join, get_python_lib(), package_name)
    to_funcs = lambda name: (
        partial(path.join, f_for(name)),
        partial(path.join, d_for(name)),
    )

    _data_join, _data_install_dir = to_funcs("data")

    with open(path.join(package_name, "__init__.py")) as f:
        __author__, __version__ = map(
            lambda buf: next(map(lambda e: e.value.s, parse(buf).body)),
            filter(
                lambda line: line.startswith("__version__")
                or line.startswith("__author__"),
                f,
            ),
        )

    setup(
        name=package_name,
        author=__author__,
        version=__version__,
        description="Postgres deployment module for Fabric (offregister)",
        classifiers=[
            "Development Status :: 7 - Inactive",
            "Intended Audience :: Developers",
            "Topic :: Software Development",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "License :: OSI Approved :: MIT License",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2.7",
        ],
        test_suite=package_name + ".tests",
        packages=find_packages(),
        package_dir={package_name: package_name},
        data_files=[
            (_data_install_dir(), list(map(_data_join, listdir(_data_join())))),
        ],
    )
