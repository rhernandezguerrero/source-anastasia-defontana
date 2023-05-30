import sys
import os
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams import (Stream, IncrementalMixin)
from airbyte_cdk.sources.streams.http import HttpStream
import requests
import collections
sys.path.append(f'{os.path.dirname(os.path.realpath(__file__))}/utils/')


class Clients(HttpStream, ABC):
    url_base = "https://api.defontana.com/api/"
    primary_key = "legalCode"
    #cursor_field = "pageNumber"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.itemsPerPage = config['itemsPerPage']
        self.pageNumber = config['pageNumber']
        self.access_token = config['access_token']
        #self._cursor_value = {}

    #@property
    #def state(self) -> Mapping[str, Any]:
    #    if self._cursor_value:
    #        return {self.cursor_field: self._cursor_value}
    #    return {self.cursor_field: None}

    #@property
    #def state_checkpoint_interval(self) -> Optional[int]:
    #    return self.limit

    #@state.setter
    #def state(self, value: Mapping[str, Any]):
    #    self._cursor_value = value[self.cursor_field]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if not response_json["clientList"]:
            return None
        return response_json['pageNumber'] + 1

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "Sale/GetClients"


    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        next_page = next_page_token or self.pageNumber
        params = {
            "status": "0",
            "itemsPerPage": self.itemsPerPage,
            "pageNumber": next_page
        }
        print(f"Requesting stream clients from page {next_page}")

        return params

    def request_headers(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}



    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        output_response = response.json()
        if output_response["clientList"]:
            for item in output_response["clientList"]:

                client_detail = {
                    "client_id": item["legalCode"],
                    "client_dni": item["legalCode"],
                    "client_email": item["email"],
                    "client_city": item["city"],
                    "client_province": "No information",
                    "client_zip": item["zipCode"],
                    "client_address": item["address"],
                    "client_accepts_marketing": "No information",
                    "client_first_name": item["name"],
                    "client_last_name": item["lastName1"],
                    "client_phone": item["phone"],
                    "client_sex": "No information",
                    "client_age": "No information"
                }
                yield client_detail
