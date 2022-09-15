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


import json
import pandas as pd
import copy


__slot_base_value = {
    #"variable_name": "",
    #"key": "",
    "available_systems": {}
}

__slot_system_value = {
    "dqa_assessment": 1,
    "data_map": "",
    "filter": "",
    "source_variable_name": "",
    "source_table_name": "",
    "constraints": "",
    "plausibility_relation": ""
}


def slot_create_dqa_value(mdr: pd.DataFrame, mdr_row: pd.Series, sqls: list):

    # begin from here to create "callable" funciton for dqa-mdr-connector
    # Every System designation within database (eg. Person.Demographie.AdministrativesGeschlecht)
    all_systems = mdr[mdr["variable_name"] == mdr_row["variable_name"]]

    # drop duplicates of source system types #maybe need a deepcopy here
    available_source_system_types = all_systems["source_system_type"].unique()

    # create base_slot here with available information which is common over all data system types
    # get json template container
    manipulate_slot_base_value = copy.deepcopy(__slot_base_value)

    # fill in variables common across system types
    #manipulate_slot_base_value["variable_name"] = mdr_row["variable_name"]
    #manipulate_slot_base_value["key"] = mdr_row["key"]

    # for each dataelement, loop over several system types that are available in the csv file
    for system_type in available_source_system_types:

        # # drop duplicates of source system names #maybe need a deepcopy here
        available_source_system_names = all_systems[
            all_systems["source_system_type"] == system_type]["source_system_name"].unique(
        )

        # subset data for source system type
        data_for_system_type = all_systems[
            (all_systems["source_system_type"] == system_type)]

        manipulate_slot_base_value["available_systems"][system_type] = {}

        # for each system type, loop over system names (different databases of one type)
        for system_name in available_source_system_names:

            # subset data for source system name
            data_for_system_name = data_for_system_type[
                (data_for_system_type["source_system_name"] == system_name)]

            if len(data_for_system_name) != 1:
                raise Exception("Error: For one distinct dataelement, here should be only \
                    one row for each 'source_system_type' and 'source_system_name'.\n \
                        Please make sure your MDR is correctly formatted. \n \
                        designation: {}\n \
                        source_system_name: {}\n \
                        source_system_type: {}".format(
                    data_for_system_type["designation"],
                    system_type,
                    system_name
                ))

            # copy json template
            manipulate_slot_system_value = copy.deepcopy(__slot_system_value)

            # fill template with system specific info
            manipulate_slot_system_value["filter"] = data_for_system_name.iloc[0]["filter"]
            manipulate_slot_system_value["source_variable_name"] = data_for_system_name.iloc[0]["source_variable_name"]
            manipulate_slot_system_value["source_table_name"] = data_for_system_name.iloc[0]["source_table_name"]
            manipulate_slot_system_value["constraints"] = data_for_system_name.iloc[0]["constraints"]
            manipulate_slot_system_value["plausibility_relation"] = data_for_system_name.iloc[0]["plausibility_relation"]
            manipulate_slot_system_value["data_map"] = data_for_system_name.iloc[0]["data_map"]
            manipulate_slot_system_value["restricting_date_var"] = data_for_system_name.iloc[0]["restricting_date_var"]
            manipulate_slot_system_value["restricting_date_format"] = data_for_system_name.iloc[0]["restricting_date_format"]
            if system_name in sqls.keys():
                if mdr_row["variable_name"] in sqls[system_name].keys():
                    manipulate_slot_system_value["sql_statement"] = sqls[system_name][mdr_row["variable_name"]]

            # append filled template to list of systems for that system type
            manipulate_slot_base_value["available_systems"][system_type][
                system_name] = manipulate_slot_system_value

    return json.dumps(manipulate_slot_base_value)
