#!/usr/bin/python

__author__ = "Lorenz A. Kapsner, Moritz Stengel"
__copyright__ = "Universitätsklinikum Erlangen"

import os
import urllib.parse as up
import pandas as pd
import posixpath
from requests.api import head
from requests.models import HTTPBasicAuth
import json
from datetime import datetime
#from tabulate import tabulate
import logging

from dqa_mdr_connector.api_connection import ApiConnector
# api doc: https://rest.demo.dataelementhub.de/swagger-ui/index.html?configUrl=/v3/api-docs/swagger-config
# dicovery doc: https://www.keycloak.org/docs/4.8/authorization_services/#_service_authorization_api


class GetMDR(ApiConnector):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)

        # initialize pandas
        self.database = pd.DataFrame(
            columns=["designation", "definition",
                     "variable_type", "constraints"]
        )

    def __call__(self):
        self.query_info_from_api()

        # finally: save pandas
        self.database.to_csv(
            path_or_buf=os.path.join(
                os.curdir,
                self.namespace_designation + "-" +
                datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
            ),
            index=False
        )

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

        # now iterate over dataelements, extract information and put into pandas
        for _dataelement_urn in namespace_dataelement_urns:
            # get data element metadata
            response, ns_dataelement_url = self.get_element_by_urn(
                urn=_dataelement_urn)

            dict_to_pandas = {
                "designation": response["definitions"][0]["designation"],
                "definition": response["definitions"][0]["definition"]
            }

            # dataelement valuedomain url
            ns_dataelement_valuedom_url = posixpath.join(
                ns_dataelement_url, "valuedomain")

            # get data element metadata
            response = self.query_api(
                url=ns_dataelement_valuedom_url,
                header=self.header
            )

            if response["type"] == "STRING":
                dict_to_pandas["variable_type"] = "string"

                dict_to_pandas["constraints"] = json.dumps(
                    {"regex": response["text"]["regEx"]
                     #  "useRegex": response["text"]["useRegEx"],
                     #  "useMaximumLength": response["text"]["useMaximumLength"],
                     #  "maximumLength": response["text"]["maximumLength"]
                     }
                )

            elif response["type"] == "NUMERIC":
                dict_to_pandas["variable_type"] = response["numeric"]["type"].lower()

                dict_to_pandas["constraints"] = json.dumps(
                    {"range": {"min": response["numeric"]["minimum"],
                               "max": response["numeric"]["maximum"],
                               "unit": response["numeric"]["unitOfMeasure"],
                               }
                     }
                )

            elif response["type"] == "DATE":
                dict_to_pandas["variable_type"] = "date"

                dict_to_pandas["constraints"] = json.dumps(
                    {"date": {"date": response["datetime"]["date"],
                              "time": response["datetime"]["time"],
                              "hourFormat": response["datetime"]["hourFormat"]}}
                )

            elif response["type"] == "DATETIME":
                dict_to_pandas["variable_type"] = "datetime"

                # dict_to_pandas["constraints"] = json.dumps(
                #     {"date": {"date": response["datetime"]["date"],
                #               "time": response["datetime"]["time"],
                #               "hourformat": response["datetime"]["hourFormat"]}}
                # )

            elif response["type"] == "BOOLEAN":
                dict_to_pandas["variable_type"] = "boolean"

            elif response["type"] == "ENUMERATED":
                dict_to_pandas["variable_type"] = "enumerated"
                dict_to_pandas["constraints"] = json.dumps(
                    {"value_set": response["permittedValues"]})

            self.database = self.database.append(
                other=dict_to_pandas,
                ignore_index=True
            )
