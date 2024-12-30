#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Iterable, List, Mapping, MutableMapping, Union

import dpath
import requests

from airbyte_cdk.sources.declarative.decoders import Decoder, JsonDecoder
from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config

# The name of the service field to bind the response (root) in each record
RECORD_ROOT_KEY = "$root"

@dataclass
class DpathExtractor(RecordExtractor):
    """
    Record extractor that searches a decoded response over a path defined as an array of fields.

    If the field path points to an array, that array is returned.
    If the field path points to an object, that object is returned wrapped as an array.
    If the field path points to an empty object, an empty array is returned.
    If the field path points to a non-existing path, an empty array is returned.

    Examples of instantiating this transform:
    ```
      extractor:
        type: DpathExtractor
        field_path:
          - "root"
          - "data"
    ```

    ```
      extractor:
        type: DpathExtractor
        field_path:
          - "root"
          - "{{ parameters['field'] }}"
    ```

    ```
      extractor:
        type: DpathExtractor
        field_path: []
    ```

    Attributes:
        field_path (Union[InterpolatedString, str]): Path to the field that should be extracted
        config (Config): The user-provided configuration as specified by the source's spec
        decoder (Decoder): The decoder responsible to transfom the response in a Mapping
    """
    field_path: List[Union[InterpolatedString, str]]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    decoder: Decoder = field(default_factory=lambda: JsonDecoder(parameters={}))

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._field_path = [
            InterpolatedString.create(path, parameters=parameters) for path in self.field_path
        ]
        for path_index in range(len(self.field_path)):
            if isinstance(self.field_path[path_index], str):
                self._field_path[path_index] = InterpolatedString.create(
                    self.field_path[path_index], parameters=parameters
                )

    def extract_records(self, response: requests.Response) -> Iterable[MutableMapping[Any, Any]]:
        for body in self.decoder.decode(response):
            root_response = body
            if len(self._field_path) == 0:
                extracted = body
            else:
                path = [path.eval(self.config) for path in self._field_path]
                if "*" in path:
                    extracted = dpath.values(body, path)
                else:
                    extracted = dpath.get(body, path, default=[])  # type: ignore # extracted will be a MutableMapping, given input data structure
            if isinstance(extracted, list):
                for record in extracted:
                    record.update({RECORD_ROOT_KEY: root_response})
                    yield record
            elif isinstance(extracted, dict):
                copy = dict(extracted)
                copy.update({RECORD_ROOT_KEY: root_response})
                yield copy
            elif extracted:
                yield extracted
            else:
                yield from []

    def remove_service_keys(self, record:MutableMapping[Any, Any], validate=False) -> MutableMapping[Any, Any]:
        """
        Remove the bindings of the service keys (like RECORD_ROOT_KEY) from the record.
        If validate is True, then make sure (assert) that the service keys existed in the record.
        """
        if validate:
            assert record[RECORD_ROOT_KEY] is not None, "Expected RECORD_ROOT_KEY service key"
        result = dict(record)
        result.pop(RECORD_ROOT_KEY)
        return result

