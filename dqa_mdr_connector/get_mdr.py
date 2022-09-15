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

import os
import pandas as pd
import posixpath
import json
#from tabulate import tabulate
import logging

from dqa_mdr_connector.api_connection import ApiConnector
from dqa_mdr_connector.slot_split import slot_split

# api doc: https://rest.demo.dataelementhub.de/swagger-ui/index.html?configUrl=/v3/api-docs/swagger-config
# dicovery doc: https://www.keycloak.org/docs/4.8/authorization_services/#_service_authorization_api


class GetMDR(ApiConnector):

    def __init__(
        self,
        output_folder="./",
        output_filename="dehub_mdr_clean.csv",
        de_fhir_paths: list = None,
        return_csv: bool = True,
        **kwargs
    ):

        super().__init__(**kwargs)

        self.de_fhir_paths = de_fhir_paths
        self.return_csv = return_csv

        self.output_folder = os.path.abspath(output_folder)
        self.output_filename = os.path.abspath(output_filename)

        # initialize pandas
        self.database = pd.DataFrame(
            columns=['designation', 'definition', 'variable_name', 'key', 'dqa_assessment',
                     'variable_type', 'source_variable_name', 'source_table_name',
                     'source_system_name', 'source_system_type', 'constraints', 'filter',
                     'data_map', 'plausibility_relation', 'restricting_date_var',
                     'restricting_date_format']
        )

    def __call__(self):
        self.query_info_from_api()

        if self.return_csv:
            self.database.to_csv(
                path_or_buf=os.path.join(
                    self.output_folder,
                    self.output_filename
                ),
                sep="\t",
                index=False
            )
        else:
            return self.database

    def query_info_from_api(self):
        ######################
        # query info from api
        ######################

        # if namespace exists, self.ns_id will be set
        self.check_if_namespace_exists()

        if self.ns_id is None:
            msg = "No or multiple namespaces found at '{}' for namespace_designation '{}'".format(
                self.base_url,
                self.namespace_designation
            )
            logging.error(msg)
            raise Exception(msg)

        namespace_dataelement_urns = self.get_namespace_urns(ns_id=self.ns_id)

        sqls = {}

        # now iterate over dataelements, extract information and put into pandas
        for _dataelement_urn in namespace_dataelement_urns:
            # get data element metadata
            response, ns_dataelement_url = self.get_element_by_urn(
                urn=_dataelement_urn)

            if not self.de_fhir_paths is None:
                fhir_path = [s for s in response["slots"]
                             if s["name"] == "fhir-path"]
                if len(fhir_path) == 1:
                    if not fhir_path[0]["value"] in self.de_fhir_paths:
                        # continue loop, if this dataelement is not wanted
                        continue
                else:
                    continue

            dict_to_pandas = {
                "designation": response["definitions"][0]["designation"],
                "definition": response["definitions"][0]["definition"]
            }

            # if fhir path not none and code arrived here (i.e. the de
            # is in self.de_fhir_path) also add the fhir-path as key
            if not self.de_fhir_paths is None and len(fhir_path) == 1:
                dict_to_pandas["key"] = fhir_path[0]["value"]
                dict_to_pandas["variable_name"] = dict_to_pandas["key"]

            # dataelement valuedomain url
            ns_dataelement_valuedom_url = posixpath.join(
                ns_dataelement_url, "valuedomain")

            # get data element metadata
            response_valuedom = self.query_api(
                url=ns_dataelement_valuedom_url,
                header=self.header
            )

            if response_valuedom["type"] == "STRING":
                dict_to_pandas["variable_type"] = response_valuedom["type"].lower()

                # dict_to_pandas["constraints"] = json.dumps(
                #     {"regex": response["text"]["regEx"]
                #      #  "useRegex": response["text"]["useRegEx"],
                #      #  "useMaximumLength": response["text"]["useMaximumLength"],
                #      #  "maximumLength": response["text"]["maximumLength"]
                #      }
                # )

            elif response_valuedom["type"] == "NUMERIC":
                dict_to_pandas["variable_type"] = response_valuedom["numeric"]["type"].lower(
                )

                # dict_to_pandas["constraints"] = json.dumps(
                #     {"range": {"min": response["numeric"]["minimum"],
                #                "max": response["numeric"]["maximum"],
                #                "unit": response["numeric"]["unitOfMeasure"],
                #                }
                #      }
                # )

            elif "DATE" in response_valuedom["type"]:
                dict_to_pandas["variable_type"] = "datetime"

                # dict_to_pandas["constraints"] = json.dumps(
                #     {"date": {"date": response["datetime"]["date"],
                #               "time": response["datetime"]["time"],
                #               "hourFormat": response["datetime"]["hourFormat"]}}
                # )

            elif response_valuedom["type"] == "BOOLEAN":
                dict_to_pandas["variable_type"] = response_valuedom["type"].lower()

            elif response_valuedom["type"] == "ENUMERATED":
                dict_to_pandas["variable_type"] = response_valuedom["type"].lower()

                # permitted_val_response = response["permittedValues"]

                # # default empty list
                # value_list = []

                # # fill value list
                # for val in permitted_val_response:
                #     value_list = value_list + [val["value"]]

                # dict_to_pandas["constraints"] = json.dumps(
                #     {"value_set": ", ".join(value_list)})

            mdr_temp = pd.DataFrame(
                data=dict_to_pandas,
                dtype=str,
                index=[0]
            )

            if len(response["slots"]) > 0:
                # until now, dict_to_pandas is one row,
                # however, when expanding slot, we can get several rows (for different
                # system types and system names) for one data element.
                # Hence, we need to transfer dict_to_pandas to a data frame and join data frame
                # from expanded slot

                for _element in response["slots"]:
                    if _element["name"] == "dqa":
                        dqa_slot = _element["value"]
                        break

                try:
                    pandas_from_slot, de_sqls = slot_split(
                        json_slot=json.loads(dqa_slot),
                        designation=dict_to_pandas["designation"],
                        definition=dict_to_pandas["definition"]
                    )
                    mdr_temp = mdr_temp.astype(str).merge(
                        pandas_from_slot.astype(str),
                        on=["designation", "definition"],
                        how="outer"
                    )

                    for _sysname in de_sqls.keys():
                        if not _sysname in sqls.keys():
                            sqls[_sysname] = {}
                        sqls[_sysname][dict_to_pandas["variable_name"]
                                       ] = de_sqls[_sysname]
                except Exception as e:
                    logging.error(e)

            self.sql_statements = sqls
            self.database = pd.concat(
                [self.database, mdr_temp],
                ignore_index=True,
                axis=0,
                join="outer"
            )
