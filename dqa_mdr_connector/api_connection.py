#!/usr/bin/python

__author__ = "Lorenz A. Kapsner, Moritz Stengel"
__copyright__ = "Universitätsklinikum Erlangen"

import getpass
import requests
from requests.api import head
from requests.models import HTTPBasicAuth
import json
import logging
import urllib.parse as up
import posixpath
# api doc: https://rest.demo.dataelementhub.de/swagger-ui/index.html?configUrl=/v3/api-docs/swagger-config
# dicovery doc: https://www.keycloak.org/docs/4.8/authorization_services/#_service_authorization_api


class ApiConnector():

    def __init__(self, api_url: str, api_auth_url: str, namespace_designation: str):

        # set base url
        self.base_url = api_url
        # set namespace designation
        self.namespace_designation = namespace_designation

        # connect to api
        self.api_connection = self.get_con(auth_url=api_auth_url)

        # get tokens from json
        json_dump = json.loads(self.api_connection.text)

        self.access_token = json_dump["access_token"]
        self.refresh_token = json_dump["refresh_token"]

        self.header = {"Authorization": "Bearer " + self.access_token}

    @staticmethod
    def get_credentials(base_url):

        username = input(
            "Please insert your username for '{}':\n".format(base_url))
        password = getpass.getpass(
            "\n\nInsert your password for username '{}':\n".format(username))

        return username, password

    def get_con(self, auth_url: str):

        # get discovery document:
        # curl -X GET https://auth.dev.osse-register.de/auth/realms/dehub-demo/.well-known/uma2-configuration

        uname, pw = self.get_credentials(base_url=self.base_url)

        data = {
            "grant_type": "password",
            "client_id": "dehub",
            "scope": "dehub",
            "username": uname,
            "password": pw
        }

        response = requests.post(
            url=auth_url,
            data=data
        )

        return response

    @staticmethod
    def query_api(url, header):
        logging.info("API call: {}".format(url))
        r = requests.get(
            url=url,
            headers=header
        )
        j = json.loads(r.text)
        return j

    def check_if_namespace_exists(self):
        # get namespaces
        response = self.query_api(
            url=self.base_url + "namespaces/",
            header=self.header
        )

        self.ns_id = None

        # print(response)

        # get namespace ID
        # solving cardinality
        for _element in response["ADMIN"]:
            # print(_element)

            for _multi_definitions in _element["definitions"]:
                if _multi_definitions["designation"] == self.namespace_designation and \
                        _element["identification"]["status"] == "RELEASED":
                    self.ns_id = str(_element["identification"]["identifier"])
                    self.ns_urn = str(_element["identification"]["urn"])
                    break

    def get_namespace_urns(self, ns_id):
        # now get all data elements of this namespace
        # set namespace/members url
        self.ns_members_url = up.urljoin(
            self.base_url, posixpath.join("namespaces", ns_id, "members"))

        # get data elements in namespace
        response = self.query_api(
            url=self.ns_members_url,
            header=self.header
        )

        # list with urns of data elements
        namespace_dataelement_urns = [
            _element["elementUrn"] for _element in response if ns_id + ":dataelement:" in _element["elementUrn"] and
            _element["status"] == "RELEASED"]

        return namespace_dataelement_urns

    def get_element_by_urn(self, urn: str):
        # dataelement base url
        ns_dataelement_url = up.urljoin(
            base=self.base_url,
            url=posixpath.join(
                "element", urn
            )
        )

        response = self.query_api(
            url=ns_dataelement_url,
            header=self.header
        )
        # get data element metadata
        return response, ns_dataelement_url
