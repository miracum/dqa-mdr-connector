#!/usr/bin/python

# dqa-mdr-connector: Connecting the MIRACUM-MDR with the DQA-Tool
# Copyright (C) 2022 Universitätsklinikum Erlangen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__author__ = "Lorenz A. Kapsner, Moritz Stengel"
__copyright__ = "Universitätsklinikum Erlangen"

# create docs from root directory:
# pdoc3 --html --output-dir doc dqa_mdr_connector --force

# create requirements from root directory:
# pipreqs ./ --force

# update news.md
# auto-changelog -u -t "dqa-mdr-connector NEWS" --tag-prefix "v" -o "NEWS.md"

from distutils.core import setup

from setuptools import find_packages

req_file = "requirements.txt"


def parse_requirements(filename):
    lineiter = (line.strip() for line in open(filename))
    return [line for line in lineiter if line and not line.startswith("#")]


install_reqs = parse_requirements(req_file)

setup(
    name='dqa_mdr_connector',
    version='0.0.2',
    author="Lorenz A. Kapsner, Moritz Stengel",
    author_email="lorenz.kapsner@uk-erlangen.de, moritz.stengel@extern.uk-erlangen.de",
    license="GPLv3",
    copyright="Universitätsklinikum Erlangen",
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=install_reqs,
    dependency_links=[],
)
