#!/usr/bin/env python

__author__ = "Lorenz A. Kapsner, Moritz Stengel"
__copyright__ = "Universitätsklinikum Erlangen"

# create docs from root directory:
# pdoc3 --html --output-dir doc tirutils --force

# create requirements from root directory:
# pipreqs ./ --force

from distutils.core import setup

from setuptools import find_packages

req_file = "requirements.txt"


def parse_requirements(filename):
    lineiter = (line.strip() for line in open(filename))
    return [line for line in lineiter if line and not line.startswith("#")]


install_reqs = parse_requirements(req_file)

setup(
    name='dqa_mdr_connector',
    version='0.0.1',
    author="Lorenz A. Kapsner, Moritz Stengel",
    author_email="lorenz.kapsner@uk-erlangen.de, moritz.stengel@extern.uk-erlangen.de",
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=install_reqs,
    dependency_links=[],
)
