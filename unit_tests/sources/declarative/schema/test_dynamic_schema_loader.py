#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.schema import DynamicSchemaLoader, SchemaTypeIdentifier
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse

_CONFIG = {
    "start_date": "2024-07-01T00:00:00.000Z",
}

_MANIFEST = {
    "version": "6.7.0",
    "definitions": {
        "party_members_stream": {
            "type": "DeclarativeStream",
            "name": "party_members",
            "primary_key": [],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.test.com",
                    "path": "/party_members",
                    "http_method": "GET",
                    "authenticator": {
                        "type": "ApiKeyAuthenticator",
                        "header": "apikey",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
                "paginator": {"type": "NoPagination"},
            },
            "schema_loader": {
                "type": "DynamicSchemaLoader",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "/party_members/schema",
                        "http_method": "GET",
                        "authenticator": {
                            "type": "ApiKeyAuthenticator",
                            "header": "apikey",
                            "api_token": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "paginator": {"type": "NoPagination"},
                },
                "schema_transformations": [
                    {
                        "type": "AddFields",
                        "fields": [
                            {
                                "type": "AddedFieldDefinition",
                                "path": ["StaticField"],
                                "value": "{{ {'type': ['null', 'string']} }}",
                            }
                        ],
                    },
                    {
                        "type": "KeysToSnakeCase",
                    },
                ],
                "schema_type_identifier": {
                    "schema_pointer": ["fields"],
                    "key_pointer": ["name"],
                    "type_pointer": ["type"],
                    "types_mapping": [
                        {"target_type": "string", "current_type": "singleLineText"},
                        {
                            "target_type": {
                                "field_type": "array",
                                "items": {"field_type": "array", "items": "integer"},
                            },
                            "current_type": "formula",
                            "condition": "{{ raw_schema['result']['type'] == 'customInteger' }}",
                        },
                    ],
                },
            },
        },
    },
    "streams": [
        "#/definitions/party_members_stream",
    ],
    "check": {"stream_names": ["party_members"]},
}


@pytest.fixture
def mock_retriever():
    retriever = MagicMock()
    retriever.read_records.return_value = [
        {
            "schema": [
                {"field1": {"key": "name", "type": "string"}},
                {"field2": {"key": "age", "type": "integer"}},
                {"field3": {"key": "active", "type": "boolean"}},
            ]
        }
    ]
    return retriever


@pytest.fixture
def mock_schema_type_identifier():
    return SchemaTypeIdentifier(
        schema_pointer=["schema"],
        key_pointer=["key"],
        type_pointer=["type"],
        types_mapping=[],
        parameters={},
    )


@pytest.fixture
def dynamic_schema_loader(mock_retriever, mock_schema_type_identifier):
    config = MagicMock()
    parameters = {}
    return DynamicSchemaLoader(
        retriever=mock_retriever,
        config=config,
        parameters=parameters,
        schema_type_identifier=mock_schema_type_identifier,
    )


@pytest.mark.parametrize(
    "retriever_data, expected_schema",
    [
        (
            # Test case: All fields with valid types
            iter(
                [
                    {
                        "schema": [
                            {"key": "name", "type": "string"},
                            {"key": "age", "type": "integer"},
                        ]
                    }
                ]
            ),
            {
                "$schema": "https://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "type": "object",
                "properties": {
                    "name": {"type": ["null", "string"]},
                    "age": {"type": ["null", "integer"]},
                },
            },
        ),
        (
            # Test case: Fields with missing type default to "string"
            iter(
                [
                    {
                        "schema": [
                            {"key": "name"},
                            {"key": "email", "type": "string"},
                        ]
                    }
                ]
            ),
            {
                "$schema": "https://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "type": "object",
                "properties": {
                    "name": {"type": ["null", "string"]},
                    "email": {"type": ["null", "string"]},
                },
            },
        ),
        (
            # Test case: Fields with nested types
            iter(
                [
                    {
                        "schema": [
                            {"key": "address", "type": ["string", "integer"]},
                        ]
                    }
                ]
            ),
            {
                "$schema": "https://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "type": "object",
                "properties": {
                    "address": {
                        "oneOf": [{"type": ["null", "string"]}, {"type": ["null", "integer"]}]
                    },
                },
            },
        ),
        (
            # Test case: Empty record set
            iter([]),
            {
                "$schema": "https://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "type": "object",
                "properties": {},
            },
        ),
    ],
)
def test_dynamic_schema_loader(dynamic_schema_loader, retriever_data, expected_schema):
    dynamic_schema_loader.retriever.read_records = MagicMock(return_value=retriever_data)

    schema = dynamic_schema_loader.get_json_schema()

    # Validate the generated schema
    assert schema == expected_schema


def test_dynamic_schema_loader_invalid_key(dynamic_schema_loader):
    # Test case: Invalid key type
    dynamic_schema_loader.retriever.read_records.return_value = iter(
        [{"schema": [{"field1": {"key": 123, "type": "string"}}]}]
    )

    with pytest.raises(ValueError, match="Expected key to be a string"):
        dynamic_schema_loader.get_json_schema()


def test_dynamic_schema_loader_invalid_type(dynamic_schema_loader):
    # Test case: Invalid type
    dynamic_schema_loader.retriever.read_records.return_value = iter(
        [{"schema": [{"field1": {"key": "name", "type": "invalid_type"}}]}]
    )

    with pytest.raises(ValueError, match="Expected key to be a string. Got None"):
        dynamic_schema_loader.get_json_schema()


def test_dynamic_schema_loader_manifest_flow():
    expected_schema = {
        "$schema": "https://json-schema.org/draft-07/schema#",
        "additionalProperties": True,
        "type": "object",
        "properties": {
            "id": {"type": ["null", "integer"]},
            "first_name": {"type": ["null", "string"]},
            "description": {"type": ["null", "string"]},
            "static_field": {"type": ["null", "string"]},
        },
    }

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
    )

    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/party_members"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "first_name": "member_1", "description": "First member"},
                        {"id": 2, "first_name": "member_2", "description": "Second member"},
                    ]
                )
            ),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/party_members/schema"),
            HttpResponse(
                body=json.dumps(
                    {
                        "fields": [
                            {"name": "Id", "type": "integer"},
                            {"name": "FirstName", "type": "string"},
                            {"name": "Description", "type": "singleLineText"},
                        ]
                    }
                )
            ),
        )

        actual_catalog = source.discover(logger=source.logger, config=_CONFIG)

    assert len(actual_catalog.streams) == 1
    assert actual_catalog.streams[0].json_schema == expected_schema


def test_dynamic_schema_loader_with_type_conditions():
    _MANIFEST_WITH_TYPE_CONDITIONS = deepcopy(_MANIFEST)
    _MANIFEST_WITH_TYPE_CONDITIONS["definitions"]["party_members_stream"]["schema_loader"][
        "schema_type_identifier"
    ]["types_mapping"].append(
        {
            "target_type": "number",
            "current_type": "formula",
            "condition": "{{ raw_schema['result']['type'] == 'number' }}",
        }
    )
    _MANIFEST_WITH_TYPE_CONDITIONS["definitions"]["party_members_stream"]["schema_loader"][
        "schema_type_identifier"
    ]["types_mapping"].append(
        {
            "target_type": "number",
            "current_type": "formula",
            "condition": "{{ raw_schema['result']['type'] == 'currency' }}",
        }
    )
    _MANIFEST_WITH_TYPE_CONDITIONS["definitions"]["party_members_stream"]["schema_loader"][
        "schema_type_identifier"
    ]["types_mapping"].append({"target_type": "array", "current_type": "formula"})

    expected_schema = {
        "$schema": "https://json-schema.org/draft-07/schema#",
        "additionalProperties": True,
        "type": "object",
        "properties": {
            "id": {"type": ["null", "integer"]},
            "first_name": {"type": ["null", "string"]},
            "description": {"type": ["null", "string"]},
            "static_field": {"type": ["null", "string"]},
            "currency": {"type": ["null", "number"]},
            "salary": {"type": ["null", "number"]},
            "working_days": {"type": ["null", "array"]},
            "avg_salary": {
                "type": ["null", "array"],
                "items": {"type": ["null", "array"], "items": {"type": ["null", "integer"]}},
            },
        },
    }
    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST_WITH_TYPE_CONDITIONS, config=_CONFIG, catalog=None, state=None
    )
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/party_members"),
            HttpResponse(
                body=json.dumps(
                    [
                        {
                            "id": 1,
                            "first_name": "member_1",
                            "description": "First member",
                            "salary": 20000,
                            "currency": 10.4,
                            "working_days": ["Monday", "Tuesday"],
                        },
                        {
                            "id": 2,
                            "first_name": "member_2",
                            "description": "Second member",
                            "salary": 22000,
                            "currency": 10.4,
                            "working_days": ["Tuesday", "Wednesday"],
                        },
                    ]
                )
            ),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/party_members/schema"),
            HttpResponse(
                body=json.dumps(
                    {
                        "fields": [
                            {"name": "Id", "type": "integer"},
                            {"name": "FirstName", "type": "string"},
                            {"name": "Description", "type": "singleLineText"},
                            {"name": "Salary", "type": "formula", "result": {"type": "number"}},
                            {
                                "name": "AvgSalary",
                                "type": "formula",
                                "result": {"type": "customInteger"},
                            },
                            {"name": "Currency", "type": "formula", "result": {"type": "currency"}},
                            {"name": "Currency", "type": "formula", "result": {"type": "currency"}},
                            {"name": "WorkingDays", "type": "formula"},
                        ]
                    }
                )
            ),
        )

        actual_catalog = source.discover(logger=source.logger, config=_CONFIG)

    assert len(actual_catalog.streams) == 1
    assert actual_catalog.streams[0].json_schema == expected_schema
