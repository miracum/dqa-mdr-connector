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


import pandas as pd


def slot_split(json_slot: dict, designation: str, definition: str):
    base_row = {}
    base_row["designation"] = designation
    base_row["definition"] = definition
    #base_row["variable_name"] = json_slot["variable_name"]
    #base_row["key"] = json_slot["key"]

    manipulate_mdr = pd.DataFrame()

    for system_type in json_slot["available_systems"].keys():
        for system_name in json_slot["available_systems"][system_type].keys():

            system_name_row = {"source_system_type": system_type,
                               "source_system_name": system_name}

            system_name_data = json_slot["available_systems"][system_type][system_name]

            system_name_row["dqa_assessment"] = str(
                system_name_data["dqa_assessment"])
            system_name_row["filter"] = system_name_data["filter"]
            system_name_row["source_variable_name"] = system_name_data["source_variable_name"]
            system_name_row["source_table_name"] = system_name_data["source_table_name"]
            system_name_row["constraints"] = system_name_data["constraints"]
            system_name_row["plausibility_relation"] = system_name_data["plausibility_relation"]
            system_name_row["data_map"] = system_name_data["data_map"]
            system_name_row["restricting_date_var"] = system_name_data["restricting_date_var"]
            system_name_row["restricting_date_format"] = system_name_data["restricting_date_format"]

            # filter, source_variable_name, source_table_name

            system_name_dict = {**base_row, **system_name_row}

            manipulate_mdr = pd.concat(
                [manipulate_mdr, pd.DataFrame(system_name_dict, index=[0])],
                ignore_index=True,
                axis=0,
                join="outer"
            )

    return manipulate_mdr
