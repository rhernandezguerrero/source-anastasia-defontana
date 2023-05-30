import sys
import os
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams import (Stream, IncrementalMixin)
from airbyte_cdk.sources.streams.http import HttpStream
import requests
import collections
from ..utils import details
#sys.path.append(f'{os.path.dirname(os.path.realpath(__file__))}/utils/')


class Products(HttpStream, ABC):
    url_base = "https://api.defontana.com/api/"
    primary_key = "legalCode"
    #cursor_field = "pageNumber"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.itemsPerPage = config['itemsPerPage']
        self.pageNumber = config['pageNumber']
        self.access_token = config['access_token']
        self.product_details = details.ProductDetails(self.access_token)
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
        if not response_json["productList"]:
            return None
        return response_json['pageNumber'] + 1

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "sale/GetProducts"

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
        print(f"Requesting stream products from page {next_page}")

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
        if output_response["productList"]:
            for item in output_response["productList"]:

                product_detail = {
                    "variant_bar_code": item["code"],
                    "sku": item["code"],
                    "variant_description": item["detailedDescription"],
                    "variant_id": "No information",
                    "product_description": item["detailedDescription"],
                    "product_id": item["code"],
                    "product_name": item["name"],
                    "product_type_name": "No information",
                    "variant_average_cost": self.product_details.get_product_cost(item["code"]),
                    "state": item["active"]
                }
                yield product_detail
