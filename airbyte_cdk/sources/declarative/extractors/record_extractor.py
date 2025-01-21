#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import requests

# Convention:
# - The record extractors may leave service fields bound in the extracted records (mappings).
# - The names (keys) of the service fields have the value of SERVICE_KEY_PREFIX as their prefix.
# - The service fields are kept only during the record's filtering and transformation.
_SERVICE_KEY_PREFIX = "$"


def add_service_key(mapping: Mapping[str, Any], key:str, value:Any) -> Mapping[str, Any]:
    """
    :param mapping: non-null mapping
    :param key: the name of the key, not including any specific prefixes
    :param value: the value to bind
    :return: a non-null copy of the mappibg including a new key-value pair, where the key is prefixed as service field.
    """
    result = dict(mapping)
    result[_SERVICE_KEY_PREFIX+key] = value
    return result


def exclude_service_keys(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
    return {k: v for k, v in mapping.items() if not is_service_key(k)}

def is_service_key(key: str) -> bool:
    return key.find(_SERVICE_KEY_PREFIX) == 0

def assert_service_keys_exist(self,mapping: Mapping[str, Any]):  # type: ignore[no-untyped-def]
    assert mapping != exclude_service_keys(mapping), "The mapping should contain service keys"

@dataclass
class RecordExtractor:
    """
    Responsible for translating an HTTP response into a list of records by extracting records from the response.
    """

    @abstractmethod
    def extract_records(
        self,
        response: requests.Response,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Selects records from the response
        :param response: The response to extract the records from
        :return: List of Records extracted from the response
        """
        pass


    def remove_service_keys(self,
                            records: Iterable[Mapping[str, Any]]
                            ) -> Iterable[Mapping[str, Any]]:
        for record in records:
            yield exclude_service_keys(record)
