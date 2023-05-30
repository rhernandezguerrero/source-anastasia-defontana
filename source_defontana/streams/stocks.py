import sys
import os
from abc import ABC
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.sources.streams import (Stream, IncrementalMixin)
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
import requests
import collections
from .base import IncrementalDefontanaStream, DefontanaSubStream


class Stocks(IncrementalDefontanaStream):
    url_base = "https://api.defontana.com/api/"
    primary_key = 'code'

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "Sale/GetStorages"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if not response_json["storageList"]:
            return None
        return response_json['pageNumber'] + 1

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
        print(f"Requesting stream storages from page {next_page}")

        return params

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        output_response = response.json()
        if output_response["storageList"]:
            for item in output_response["storageList"]:
                yield item


class StockDetails(DefontanaSubStream):
    parent_stream_class: object = Stocks
    primary_key = 'code'
    slice_key = 'code'

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config, **kwargs)
        self.config = config
        self.parent_id = None
        self.date = date.today().strftime("%Y-%m-%d")

    @property
    def parent_stream(self) -> object:

        return self.parent_stream_class(self.config) if self.parent_stream_class else None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        self.parent_id = stream_slice["code"]
        return f"Sale/GetStorageStock?storageId={self.parent_id}"

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            **kwargs
    ) -> Iterable[Mapping]:
        output_response = response.json()["productList"]
        if output_response:
            for item in output_response:
                result = {
                    "quantity_available": item["stock"],
                    "variant_id": item["productID"],
                    "sku": item["productID"],
                    "store_id": self.parent_id,
                    "date": self.date
                }
                yield result

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if not response_json["productList"]:
            return None
        return response_json['pageNumber'] + 1


    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
            **kwargs
    ) -> MutableMapping[str, Any]:
        next_page = next_page_token or self.pageNumber
        params = {
            "status": "0",
            "itemsPerPage": self.itemsPerPage,
            "pageNumber": next_page
        }
        print(f"Requesting stream stock_details from page {next_page}")

        return params
