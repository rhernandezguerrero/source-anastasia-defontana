import sys
import os
from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.sources.streams import (Stream, IncrementalMixin)
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
import requests
import collections

sys.path.append(f'{os.path.dirname(os.path.realpath(__file__))}/utils/')


class DefontanaStream(HttpStream, ABC):
    url_base = 'https://api.defontana.com/api/'
    primary_key = 'code'

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.itemsPerPage = config['itemsPerPage']
        self.pageNumber = config['pageNumber']
        self.access_token = config['access_token']

    def request_headers(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}


class IncrementalDefontanaStream(DefontanaStream, ABC):
    # cursor_field = 0

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config, **kwargs)
        self.itemsPerPage = config['itemsPerPage']
        self.pageNumber = config['pageNumber']
        self.access_token = config['access_token']

    # @property
    # def state_checkpoint_interval(self) -> int:
    #    return self.skip

    # @property
    # def default_state_comparison_value(self) -> Union[int, str]:
    # certain streams are using `id` field as `cursor_field`, which requires to use `int` type,
    # but many other use `str` values for this, we determine what to use based on `cursor_field` value
    #    return 0 if self.cursor_field == "ID" else ""

    # def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
    #    return {
    #        self.cursor_field: max(
    #            latest_record.get(self.cursor_field, self.default_state_comparison_value),
    #            current_stream_state.get(self.cursor_field, self.default_state_comparison_value),
    #        )
    #    }


class DefontanaSubStream(DefontanaStream):
    primary_key = 'code'

    parent_stream_class: object = None
    slice_key: str = None
    nested_record: str = "code"
    nested_record_field_name: str = None
    nested_substream = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config, **kwargs)

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        parent_stream_state = stream_state.get(self.parent_stream.name) if stream_state else {}
        
        for record in self.parent_stream.read_records(stream_state=parent_stream_state, **kwargs):
            if self.nested_substream:
                if record.get(self.nested_substream):
                    yield {self.slice_key: record[self.nested_record]}
            else:
                yield {self.slice_key: record[self.nested_record]}

    def read_records(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Optional[Mapping[str, Any]] = None,
            **kwargs,
    ) -> Iterable[Mapping[str, Any]]:

        slice_data = stream_slice.get(self.slice_key)

        if isinstance(slice_data, list) and self.nested_record_field_name is not None and len(slice_data) > 0:
            slice_data = slice_data[0].get(self.nested_record_field_name)

        self.logger.info(f"Reading {self.name} for {self.slice_key}: {slice_data}")
        records = super().read_records(stream_slice=stream_slice, **kwargs)
        yield from self.filter_records_newer_than_state(stream_state=stream_state, records_slice=records)

    def filter_records_newer_than_state(self, stream_state: Mapping[str, Any] = None, records_slice: Mapping[str, Any] = None) -> Iterable:
        for record in records_slice:
            yield record

