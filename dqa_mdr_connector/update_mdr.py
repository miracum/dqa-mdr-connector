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
import requests
import urllib.parse as up
import pandas as pd
import posixpath
import json
import logging
import copy
from pathlib import Path

from dqa_mdr_connector.api_connection import ApiConnector
from dqa_mdr_connector.slot_create import slot_create_dqa_value

# api doc: https://rest.demo.dataelementhub.de/swagger-ui/index.html?configUrl=/v3/api-docs/swagger-config
# dicovery doc: https://www.keycloak.org/docs/4.8/authorization_services/#_service_authorization_api


class UpdateMDR(ApiConnector):

    def __init__(
            self,
            csv_file: str,
            sql_dir: str = None,
            separator: str = ",",
            main_system_name: str = "i2b2",
            main_system_type: str = "postgres",
            de_fhir_paths: list = None,
            **kwargs
    ):

        # if creating new namespace, arg "namespace_definition" is required
        if "namespace_definition" in kwargs.keys():
            self.namespace_definition = kwargs.pop("namespace_definition")

        # initialize apiconnector
        super().__init__(download=False, **kwargs)

        self.de_fhir_paths = de_fhir_paths

        self.csv_file_name = csv_file

        # init templates
        self.init_templates()

        # read sqls
        if self.sql_dir is not None:
            self.sqls = self._init_sqls(sql_dir)
        else:
            self.sqls = None

        # read database
        self.read_csv_mdr(separator=separator)

        # MDR = self.database
        # now create main_system_mdr with unique dataelements only (no duplicate designation)
        # as defined by arg 'main_system_name' and 'main_system_type'
        self.main_system_mdr = self.database[
            (self.database["source_system_name"] == main_system_name) &
            (self.database["source_system_type"] == main_system_type)]

        if len(self.main_system_mdr) != \
                len(self.main_system_mdr[["designation", "key", "variable_name"]]):
            raise Exception(
                "main_system_mdr contains duplicate entries of data elements.")

    def __call__(self):

        # define some empty containers for later
        mdr_de_designations = {}

        # test, if namespace already exists in remote-mdr
        # if namespace exists, self.ns_id is set
        self.check_if_namespace_exists()

        if self.ns_id is None:
            logging.info("Namespace '' does not exist.\n".format(
                self.namespace_designation))

            msg = "No or multiple namespaces found at '{}' for namespace_designation '{}'.\n \
                Creating new namespace.".format(
                self.base_url,
                self.namespace_designation
            )
            logging.warning(msg)

            create_ns = {
                "identification": {
                    "elementType": "NAMESPACE",
                    "hideNamespace": True,
                    "status": "RELEASED"
                },
                "definitions": [
                    {
                        "designation": self.namespace_designation,
                        "definition": self.namespace_definition,
                        "language": "en"
                    }
                ]
            }

            response = requests.post(
                url=self.base_url + "namespaces/",
                data=json.dumps(create_ns),
                headers=self.header
            )

            # log response
            logging.info(response)

            # now, namespace exists, set self.ns_id
            self.check_if_namespace_exists()

        else:
            logging.info("Namespace '{}' already exists.\n".format(
                self.namespace_designation))
            namespace_dataelement_urns = self.get_namespace_urns(
                ns_id=self.ns_id)

            urn_designation_mapping = {}

            # get designation for each urn
            for _dataelement_urn in namespace_dataelement_urns:
                # get data element from mdr
                response, ns_dataelement_url = self.get_element_by_urn(
                    urn=_dataelement_urn
                )

                # check for fhir path
                fhir_path = [s for s in response["slots"] if s["name"] == "fhir-path"]
                if len(fhir_path) == 1:
                    if not fhir_path[0]["value"] in self.de_fhir_paths:
                        # continue loop, if this dataelement is not wanted
                        continue
                else:
                    continue

                multi_designation_list = []

                for _element in response["definitions"]:
                    multi_designation_list.append(_element["designation"])

                urn_designation_mapping[_dataelement_urn] = {
                    "designation": multi_designation_list,
                    "valueDomainUrn": response["valueDomainUrn"]
                }

            # lookup -> get elements of csv-file that are already present in mdr
            # mdr data element designations
            for _urn, _de_designations in urn_designation_mapping.items():
                while True:
                    for _de_designation in _de_designations["designation"]:
                        if _de_designation in self.main_system_mdr["designation"].values:
                            mdr_de_designations[_de_designation] = _urn
                            # if correct designation found within set of designations,
                            # skip for-loop
                            break
                    break

        # update existing dataelements
        for _i, _row in self.main_system_mdr.iterrows():
            _designation = _row["designation"]
            _definition = _row["definition"]

            logging.info("Dataelement: {}\n\n".format(_designation))

            # define basic json container
            de_basetemp = copy.deepcopy(self._de_json_template)
            # get _ns_urn elswhere, write to "self.ns_urn"
            de_basetemp["identification"]["namespaceUrn"] = self.ns_urn

            # fill definition template
            de_definition_temp = copy.deepcopy(
                self._de_definition_json_template)
            de_definition_temp["designation"] = _designation
            de_definition_temp["definition"] = _definition

            # add definition template to basetemp
            de_basetemp["definitions"].append(de_definition_temp)

            # create and modify temporary slot list element
            # (which is actually our dict from the slot_template)
            create_slot_tmp = copy.deepcopy(
                self._de_slot_template
            )
            create_slot_tmp["name"] = "dqa"
            create_slot_tmp["value"] = slot_create_dqa_value(
                mdr=self.database,
                mdr_row=_row,
                sqls=self.sqls
            )

            # append slot_temp to slots-list
            de_basetemp["slots"] = de_basetemp["slots"] + [create_slot_tmp]

            if _designation in mdr_de_designations.keys():
                # update data element on API (PUT)
                _urn = mdr_de_designations[_designation]
                _valuedomainurn = urn_designation_mapping[_urn]["valueDomainUrn"]
                de_basetemp["valueDomainUrn"] = _valuedomainurn

                # get all existing slots but the "dqa"-slot
                response, ns_dataelement_url = self.get_element_by_urn(
                    urn=_urn
                )
                de_slot_items = [s for s in response["slots"] if s["name"] != "dqa"]

                de_basetemp["slots"] = de_basetemp["slots"] + de_slot_items

                element_url = up.urljoin(
                    self.base_url,
                    posixpath.join(
                        "element",
                        _urn
                    )
                )
                response = requests.put(
                    url=element_url,
                    data=json.dumps(de_basetemp),
                    headers=self.header
                )

            else:
                # create new data element on API (POST)
                # fill valuetype
                valuedomain_temp = copy.deepcopy(eval(
                    "self._de_valuedomain_template_" +
                    _row["variable_type"]
                ))

                # json.loads()
                try:
                    _constraints = json.loads(_row["constraints"])

                    if _row["variable_type"] == "string":
                        # do some logic here to fill string-specific field in template
                        valuedomain_temp["text"]["regEx"] = _constraints["regex"]
                        valuedomain_temp["text"]["useRegEx"] = True

                    elif _row["variable_type"] == "datetime":
                        # do some logic here to fill datetime-specific field in template
                        valuedomain_temp["datetime"]["date"] = _constraints["date"]["date"]
                        valuedomain_temp["datetime"]["time"] = _constraints["date"]["time"]
                        valuedomain_temp["datetime"]["hourFormat"] = _constraints["date"]["hourFormat"]

                    elif _row["variable_type"] == "enumerated":
                        # do some logic here to fill enumerated-specific field in template
                        value_set = _constraints["value_set"].split(
                            ", ")

                        # init enumerated-form here:
                        enumerated_value_set = []

                        # loop over value_set
                        for _val in value_set:
                            enumerated_value_set.append({
                                "definitions": [{
                                    "designation": _val,
                                    "definition": _val,
                                    "language": "en"
                                }],
                                "value": _val
                            })

                        valuedomain_temp["permittedValues"] = enumerated_value_set

                    elif _row["variable_type"] in ["float", "integer"]:
                        valuedomain_temp["numeric"]["type"] = _row["variable_type"].upper(
                        )
                        # fill numeric-specific field in template
                        valuedomain_temp["numeric"]["minimum"] = _constraints["range"]["min"]
                        valuedomain_temp["numeric"]["maximum"] = _constraints["range"]["max"]
                        valuedomain_temp["numeric"]["unitOfMeasure"] = _constraints["range"]["unit"]

                        valuedomain_temp["numeric"]["useMinimum"] = True
                        valuedomain_temp["numeric"]["useMaximum"] = True

                    # add definition template to basetemp
                    de_basetemp["valueDomain"] = valuedomain_temp

                except Exception as e:
                    logging.error(e)
                    valuedomain_temp = copy.deepcopy(
                        self._de_valuedomain_template_)
                    valuedomain_temp["text"]["useRegEx"] = False
                    de_basetemp["valueDomain"] = valuedomain_temp

                element_url = up.urljoin(
                    self.base_url,
                    "element"
                )
                response = requests.post(
                    url=element_url,
                    data=json.dumps(de_basetemp),
                    headers=self.header
                )
                logging.info(response)

    def read_csv_mdr(self, separator: str):
        if separator not in [";", ","]:
            msg = "Separator of CSV-file must be ';' or ','"
            logging.error(msg)
            raise Exception(msg)
        self.database = pd.read_csv(
            filepath_or_buffer=self.csv_file_name,
            sep=separator,
            keep_default_na=False
        )

    @staticmethod
    def post_to_api(url, data, header):
        logging.info("API post: {}".format(url))
        r = requests.post(
            url=url,
            data=data,
            headers=header
        )
        return r

    def init_template_elements(self):

        self._de_definition_json_template = {
            "designation": [],
            "definition": [],
            "language": "en"
        }

        self._de_valuedomain_template_string = {
            "type": "STRING",
            "text": {
                "useRegEx": "",
                "regEx": "",
                "useMaximumLength": True,
                "maximumLength": int()
            }
        }

        # use "string", when no "variable_type" defined
        self._de_valuedomain_template_ = self._de_valuedomain_template_string

        # update datetime template correctly #done
        # above: fill correctly with info from csv
        self._de_valuedomain_template_datetime = {
            "type": "DATETIME",
            "datetime": {
                "date": "",
                "time": "",
                "hourFormat": ""
            }
        }

        # update enumerated template correctly #not possible in current status
        # above: fill correctly with info from csv
        self._de_valuedomain_template_enumerated = {
            "type": "ENUMERATED",
            "permittedValues": ""
        }

        # update float template correctly
        # above: fill correctly with info from csv
        self._de_valuedomain_template_float = {
            "type": "NUMERIC",
            "numeric": {
                "type": "",
                "useMinimum": "",  # bool
                "useMaximum": "",  # bool
                "unitOfMeasure": "",  # string
                "minimum": "",  # numeric/float
                "maximum": ""  # numeric/float
            }
        }

        self._de_valuedomain_template_integer = self._de_valuedomain_template_float

        self._de_slot_template = {
            "name": "",
            "value": ""
        }

    def init_templates(self):
        # init template elements
        self.init_template_elements()

        # data element template
        self._de_json_template = {
            "identification": {
                "elementType": "DATAELEMENT",
                "namespaceUrn": "",
                "status": "RELEASED"
            },
            "definitions": [],
            "slots": [],
            "conceptAssociations": []
        }
    
    @staticmethod
    def _init_sqls(sql_dir):
        if not os.path.exists(sql_dir) or not os.path.isdir(sql_dir):
            raise Exception("Path '{}' does not exists.")

        crawl_dir = Path(sql_dir)
        sql_files = list(
            [_file.parts[-1] for _file in crawl_dir.glob("*.JSON")]
        )
        sql_files = [_file.replace("SQL_", "") for _file in sql_files]
        sql_files = [_file.replace(".JSON", "") for _file in sql_files]

        sql_dict = {}
        for _sysname in sql_files:
            if "legacy" in _sysname:
                continue
            _sql_dir = os.path.join(
                sql_dir,
                "SQL_" + _sysname + ".JSON"
            )
            with open(_sql_dir, 'r') as j:
                sql_dict[_sysname] = json.load(j)
        
        return sql_dict
