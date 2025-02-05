#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import codecs
import logging
from dataclasses import InitVar, dataclass
from gzip import decompress
from typing import Any, Generator, List, Mapping, MutableMapping, Optional

import orjson
import requests

from airbyte_cdk.sources.declarative.decoders.decoder import Decoder

logger = logging.getLogger("airbyte")


@dataclass
class JsonDecoder(Decoder):
    """
    Decoder strategy that returns the json-encoded content of a response, if any.
    """

    parameters: InitVar[Mapping[str, Any]]

    def is_stream_response(self) -> bool:
        return False

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Given the response is an empty string or an emtpy list, the function will return a generator with an empty mapping.
        """
        try:
            body_json = response.json()
            yield from self.parse_body_json(body_json)
        except requests.exceptions.JSONDecodeError as ex:
            logger.warning("Response cannot be parsed into json: %s", ex)
            logger.debug("Response to parse: %s", response.text, exc_info=True, stack_info=True)
            yield {}  # Keep the exiting contract

    @staticmethod
    def parse_body_json(
        body_json: MutableMapping[str, Any] | List[MutableMapping[str, Any]],
    ) -> Generator[MutableMapping[str, Any], None, None]:
        if isinstance(body_json, list):
            yield from body_json
        else:
            yield from [body_json]


@dataclass
class IterableDecoder(Decoder):
    """
    Decoder strategy that returns the string content of the response, if any.
    """

    parameters: InitVar[Mapping[str, Any]]

    def is_stream_response(self) -> bool:
        return True

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        for line in response.iter_lines():
            yield {"record": line.decode()}


@dataclass
class JsonlDecoder(Decoder):
    """
    Decoder strategy that returns the json-encoded content of the response, if any.
    """

    parameters: InitVar[Mapping[str, Any]]

    def is_stream_response(self) -> bool:
        return True

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        # TODO???: set delimiter? usually it is `\n` but maybe it would be useful to set optional?
        #  https://github.com/airbytehq/airbyte-internal-issues/issues/8436
        for record in response.iter_lines():
            yield orjson.loads(record)


@dataclass
class GzipJsonDecoder(JsonDecoder):
    encoding: Optional[str]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if self.encoding:
            try:
                codecs.lookup(self.encoding)
            except LookupError:
                raise ValueError(
                    f"Invalid encoding '{self.encoding}'. Please check provided encoding"
                )

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        raw_string = decompress(response.content).decode(encoding=self.encoding or "utf-8")
        yield from self.parse_body_json(orjson.loads(raw_string))
