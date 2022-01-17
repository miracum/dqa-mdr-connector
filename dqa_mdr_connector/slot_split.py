#!/usr/bin/python

__author__ = "Lorenz A. Kapsner, Moritz Stengel"
__copyright__ = "Universitätsklinikum Erlangen"


import pandas as pd


def slot_split(json_slot: dict, designation: str, definition: str):
    base_row = {}
    base_row["designation"] = designation
    base_row["definition"] = definition
    base_row["variable_name"] = json_slot["variable_name"]
    base_row["key"] = json_slot["key"]

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

            # filter, source_variable_name, source_table_name

            system_name_dict = {**base_row, **system_name_row}

            manipulate_mdr = manipulate_mdr.append(
                other=system_name_dict,
                ignore_index=True
            )

    return manipulate_mdr
