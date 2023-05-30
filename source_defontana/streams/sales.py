import sys
import os
from abc import ABC
from datetime import datetime, date
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams import (Stream, IncrementalMixin)
from airbyte_cdk.sources.streams.http import HttpStream
import requests
import collections
sys.path.append(f'{os.path.dirname(os.path.realpath(__file__))}/utils/')


class Sales(HttpStream, ABC):
    url_base = "https://api.defontana.com/api/"
    primary_key = "legalCode"
    #cursor_field = "pageNumber"

    def __init__(self, config: Mapping[str, Any], start_date, **kwargs):
        super().__init__()
        self.itemsPerPage = config['itemsPerPage']
        self.pageNumber = config['pageNumber']
        self.access_token = config['access_token']
        self.start_date = start_date
        self.ending_date = date.today().strftime('%Y-%m-%d')
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
        if not response_json["saleList"]:
            return None
        if response_json['pageNumber'] == 3:
            pass
            #return None
        return response_json['pageNumber'] + 1

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "Sale/GetSaleByDate"


    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        next_page = next_page_token or self.pageNumber
        params = {
            "itemsPerPage": self.itemsPerPage,
            "pageNumber": next_page,
            "initialDate": self.start_date,
            "endingDate": self.ending_date,
        }
        print(f"Requesting stream sales from page {next_page}")

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
        if output_response["saleList"]:
            for sale in output_response["saleList"]:
                if sale["details"]:
                    for product in sale["details"]:
                        sale_detail = {
                            "emission_dale": datetime.strptime(sale["emissionDate"], '%Y-%m-%dT%H:%M:%S').date(),
                            "sku": product["code"],
                            "quantity": product["count"],
                            "document_id": sale["firstFolio"],
                            "document_number": sale["firstFolio"],
                            "store_id": sale["shopId"],
                            "store_name": sale["shopId"],
                            "price": float(product["total"]/product["count"]),
                            "net_unit_value": 0,
                            "total_amount": product["total"],
                            "total_net_amount": 0,
                            "total_gross_dicount": 0,
                            "client_id": sale["clientFile"],
                            "variant_id": product["code"]
                        }
                        yield sale_detail
