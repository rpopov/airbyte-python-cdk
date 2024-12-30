#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import requests


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

    def remove_service_keys(self, record: Mapping[str, Any], validate=False) -> Mapping[str, Any]:
        """
        Remove the bindings of the service keys (like RESPONSE_ROOT_KEY) from the record.
        If validate is True, then make sure (assert) that the service keys existed in the record.
        Used mostly in the tests and validations.
        """
        return record