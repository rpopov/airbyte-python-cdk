#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import gzip
import json
import os

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders import GzipJsonDecoder
from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder, JsonlDecoder


@pytest.mark.parametrize(
    "response_body, expected_json",
    [
        ("", [{}]),  # The JSON contract is irregular
        ("{}", [{}]),
        ("[]", []),
        ('{"healthcheck": {"status": "ok"}}', [{"healthcheck": {"status": "ok"}}]),
    ],
)
def test_json_decoder(requests_mock, response_body, expected_json):
    requests_mock.register_uri("GET", "https://airbyte.io/", text=response_body)
    response = requests.get("https://airbyte.io/")
    assert list(JsonDecoder(parameters={}).decode(response)) == expected_json


@pytest.mark.parametrize(
    "response_body, expected_json",
    [
        ("", []),
        ("{}", [{}]),
        ("[]", [[]]),
        ('{"id": 1, "name": "test1"}', [{"id": 1, "name": "test1"}]),
        (
            '{"id": 1, "name": "test1"}\n{"id": 2, "name": "test2"}',
            [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
        ),
    ],
    ids=[
        "empty_response",
        "empty_object_response",
        "empty_list_response",
        "one_line_json",
        "multi_line_json",
    ],
)
def test_jsonl_decoder(requests_mock, response_body, expected_json):
    requests_mock.register_uri("GET", "https://airbyte.io/", text=response_body)
    response = requests.get("https://airbyte.io/")
    assert list(JsonlDecoder(parameters={}).decode(response)) == expected_json


@pytest.fixture(name="large_events_response")
def large_event_response_fixture():
    data = {"email": "email1@example.com"}
    jsonl_string = f"{json.dumps(data)}\n"
    lines_in_response = 2_000_000  # ≈ 58 MB of response
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = f"{dir_path}/test_response.txt"
    with open(file_path, "w") as file:
        for _ in range(lines_in_response):
            file.write(jsonl_string)
    yield (lines_in_response, file_path)
    os.remove(file_path)


@pytest.mark.parametrize(
    "encoding",
    [
        "utf-8",
        "utf",
    ],
    ids=["utf-8", "utf"],
)
def test_gzipjson_decoder(requests_mock, encoding):
    response_to_compress = json.dumps(
        [
            {
                "campaignId": 214078428,
                "campaignName": "sample-campaign-name-214078428",
                "adGroupId": "6490134",
                "adId": "665320125",
                "targetId": "791320341",
                "asin": "G000PSH142",
                "advertisedAsin": "G000PSH142",
                "keywordBid": "511234974",
                "keywordId": "965783021",
            },
            {
                "campaignId": 44504582,
                "campaignName": "sample-campaign-name-44504582",
                "adGroupId": "6490134",
                "adId": "665320125",
                "targetId": "791320341",
                "asin": "G000PSH142",
                "advertisedAsin": "G000PSH142",
                "keywordBid": "511234974",
                "keywordId": "965783021",
            },
            {
                "campaignId": 509144838,
                "campaignName": "sample-campaign-name-509144838",
                "adGroupId": "6490134",
                "adId": "665320125",
                "targetId": "791320341",
                "asin": "G000PSH142",
                "advertisedAsin": "G000PSH142",
                "keywordBid": "511234974",
                "keywordId": "965783021",
            },
            {
                "campaignId": 231712082,
                "campaignName": "sample-campaign-name-231712082",
                "adGroupId": "6490134",
                "adId": "665320125",
                "targetId": "791320341",
                "asin": "G000PSH142",
                "advertisedAsin": "G000PSH142",
                "keywordBid": "511234974",
                "keywordId": "965783021",
            },
            {
                "campaignId": 895306040,
                "campaignName": "sample-campaign-name-895306040",
                "adGroupId": "6490134",
                "adId": "665320125",
                "targetId": "791320341",
                "asin": "G000PSH142",
                "advertisedAsin": "G000PSH142",
                "keywordBid": "511234974",
                "keywordId": "965783021",
            },
        ]
    )
    body = gzip.compress(response_to_compress.encode(encoding))

    requests_mock.register_uri("GET", "https://airbyte.io/", content=body)
    response = requests.get("https://airbyte.io/")
    assert len(list(GzipJsonDecoder(parameters={}, encoding=encoding).decode(response))) == 5
