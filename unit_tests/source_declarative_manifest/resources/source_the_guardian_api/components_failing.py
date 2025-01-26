#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

import requests

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.paginators import PaginationStrategy
from airbyte_cdk.sources.declarative.types import Config, Record


class IntentionalException(Exception):
    """This exception is raised intentionally in order to test error handling."""


@dataclass
class CustomPageIncrement(PaginationStrategy):
    """
    Starts page from 1 instead of the default value that is 0. Stops Pagination when currentPage is equal to totalPages.
    """

    config: Config
    page_size: Optional[Union[str, int]]
    parameters: InitVar[Mapping[str, Any]]
    start_from_page: int = 0
    inject_on_first_request: bool = False

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if isinstance(self.page_size, int) or (self.page_size is None):
            self._page_size = self.page_size
        else:
            page_size = InterpolatedString(self.page_size, parameters=parameters).eval(self.config)
            if not isinstance(page_size, int):
                raise Exception(f"{page_size} is of type {type(page_size)}. Expected {int}")
            self._page_size = page_size

    @property
    def initial_token(self) -> Optional[Any]:
        raise IntentionalException()

    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Optional[Record],
        last_page_token_value: Optional[Any],
    ) -> Optional[Any]:
        raise IntentionalException()

    def get_page_size(self) -> Optional[int]:
        return self._page_size
