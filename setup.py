# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import codecs
import os

from setuptools import find_packages
from setuptools import setup


# read file content
def read(*parts):
    path = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(path, encoding="utf-8") as fobj:
        return fobj.read()


# required modules
install_requires = [
    "croniter>=1.1.0",
    "unidecode>=1.3.2",
    "lxml>=4.9.2",
    "click>=8.1.3",
    "pyyaml>=6.0",
    "setuptools_scm>=6.0.1",
    "requests>=2.27.1",
    "jinja2>=3.1.2",
]

setup(
    name="yamc-server",
    use_scm_version={"root": ".", "relative_to": __file__, "local_scheme": "node-and-timestamp"},
    description="Yet Another Metric Collector",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Tomas Vitvar",
    author_email="tomas@vitvar.com",
    py_modules=["yamc"],
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=install_requires,
    python_requires=">=3.11.3",
    scripts=["bin/yamc"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points="""
        [console_scripts]
        yamc=yamc.commands.yamc:yamc
    """,
)
