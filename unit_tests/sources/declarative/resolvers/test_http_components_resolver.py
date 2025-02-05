#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from airbyte_cdk.models import Type
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.resolvers import (
    ComponentMappingDefinition,
    HttpComponentsResolver,
)
from airbyte_cdk.sources.embedded.catalog import (
    to_configured_catalog,
    to_configured_stream,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_CONFIG = {"start_date": "2024-07-01T00:00:00.000Z"}

_MANIFEST = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": {
                "type": "DeclarativeStream",
                "name": "",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "properties": {
                            "ABC": {"type": "number"},
                            "AED": {"type": "number"},
                        },
                        "type": "object",
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "$parameters": {"item_id": ""},
                        "url_base": "https://api.test.com",
                        "path": "/items/{{parameters['item_id']}}",
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
            },
            "components_resolver": {
                "type": "HttpComponentsResolver",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "items",
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
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{components_values['name']}}",
                    },
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": [
                            "retriever",
                            "requester",
                            "$parameters",
                            "item_id",
                        ],
                        "value": "{{components_values['id']}}",
                    },
                ],
            },
        }
    ],
}

_MANIFEST_WITH_DUPLICATES = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": {
                "type": "DeclarativeStream",
                "name": "",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "properties": {
                            "ABC": {"type": "number"},
                            "AED": {"type": "number"},
                        },
                        "type": "object",
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "$parameters": {"item_id": ""},
                        "url_base": "https://api.test.com",
                        "path": "/items/{{parameters['item_id']}}",
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
            },
            "components_resolver": {
                "type": "HttpComponentsResolver",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "duplicates_items",
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
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{components_values['name']}}",
                    },
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": [
                            "retriever",
                            "requester",
                            "$parameters",
                            "item_id",
                        ],
                        "value": "{{components_values['id']}}",
                    },
                ],
            },
        }
    ],
}

_MANIFEST_WITH_HTTP_COMPONENT_RESOLVER_WITH_RETRIEVER_WITH_PARENT_STREAM = {
    "version": "6.7.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": {
                "type": "DeclarativeStream",
                "name": "",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "properties": {
                            "ABC": {"type": "number"},
                            "AED": {"type": "number"},
                        },
                        "type": "object",
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "",
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
            },
            "components_resolver": {
                "type": "HttpComponentsResolver",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "parent/{{ stream_partition.parent_id }}/items",
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
                    "partition_router": {
                        "type": "SubstreamPartitionRouter",
                        "parent_stream_configs": [
                            {
                                "type": "ParentStreamConfig",
                                "parent_key": "id",
                                "partition_field": "parent_id",
                                "stream": {
                                    "type": "DeclarativeStream",
                                    "name": "parent",
                                    "retriever": {
                                        "type": "SimpleRetriever",
                                        "requester": {
                                            "type": "HttpRequester",
                                            "url_base": "https://api.test.com",
                                            "path": "/parents",
                                            "http_method": "GET",
                                            "authenticator": {
                                                "type": "ApiKeyAuthenticator",
                                                "header": "apikey",
                                                "api_token": "{{ config['api_key'] }}",
                                            },
                                        },
                                        "record_selector": {
                                            "type": "RecordSelector",
                                            "extractor": {
                                                "type": "DpathExtractor",
                                                "field_path": [],
                                            },
                                        },
                                    },
                                    "schema_loader": {
                                        "type": "InlineSchemaLoader",
                                        "schema": {
                                            "$schema": "http://json-schema.org/schema#",
                                            "properties": {"id": {"type": "integer"}},
                                            "type": "object",
                                        },
                                    },
                                },
                            }
                        ],
                    },
                },
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "parent_{{stream_slice['parent_id']}}_{{components_values['name']}}",
                    },
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": [
                            "retriever",
                            "requester",
                            "path",
                        ],
                        "value": "{{ stream_slice['parent_id'] }}/{{ components_values['id'] }}",
                    },
                ],
            },
        }
    ],
}


@pytest.mark.parametrize(
    "components_mapping, retriever_data, stream_template_config, expected_result",
    [
        (
            [
                ComponentMappingDefinition(
                    field_path=[InterpolatedString.create("key1", parameters={})],
                    value="{{components_values['key1']}}",
                    value_type=str,
                    parameters={},
                )
            ],
            [{"key1": "updated_value1", "key2": "updated_value2"}],
            {"key1": None, "key2": None},
            [{"key1": "updated_value1", "key2": None}],
        )
    ],
)
def test_http_components_resolver(
    components_mapping, retriever_data, stream_template_config, expected_result
):
    mock_retriever = MagicMock()
    mock_retriever.read_records.return_value = retriever_data
    mock_retriever.stream_slices.return_value = [{}]
    config = {}

    resolver = HttpComponentsResolver(
        retriever=mock_retriever,
        config=config,
        components_mapping=components_mapping,
        parameters={},
    )

    result = list(resolver.resolve_components(stream_template_config=stream_template_config))
    assert result == expected_result


def test_wrong_stream_name_type():
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/int_items"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "name": 1},
                        {"id": 2, "name": 2},
                    ]
                )
            ),
        )

        manifest = deepcopy(_MANIFEST)
        manifest["dynamic_streams"][0]["components_resolver"]["retriever"]["requester"]["path"] = (
            "int_items"
        )

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=None
        )
        with pytest.raises(ValueError) as exc_info:
            source.discover(logger=source.logger, config=_CONFIG)

        assert str(exc_info.value) == "Expected stream name 1 to be a string, got <class 'int'>."


@pytest.mark.parametrize(
    "components_mapping, retriever_data, stream_template_config, expected_result",
    [
        (
            [
                ComponentMappingDefinition(
                    field_path=[InterpolatedString.create("path", parameters={})],
                    value="{{stream_slice['parent_id']}}/{{components_values['id']}}",
                    value_type=str,
                    parameters={},
                )
            ],
            [{"id": "1", "field1": "data1"}, {"id": "2", "field1": "data2"}],
            {"path": None},
            [{"path": "1/1"}, {"path": "1/2"}, {"path": "2/1"}, {"path": "2/2"}],
        )
    ],
)
def test_http_components_resolver_with_stream_slices(
    components_mapping, retriever_data, stream_template_config, expected_result
):
    mock_retriever = MagicMock()
    mock_retriever.read_records.return_value = retriever_data
    mock_retriever.stream_slices.return_value = [{"parent_id": 1}, {"parent_id": 2}]
    config = {}

    resolver = HttpComponentsResolver(
        retriever=mock_retriever,
        config=config,
        components_mapping=components_mapping,
        parameters={},
    )

    result = list(resolver.resolve_components(stream_template_config=stream_template_config))
    assert result == expected_result


def test_dynamic_streams_read_with_http_components_resolver():
    expected_stream_names = {"item_1", "item_2"}
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "name": "item_1"},
                        {"id": 2, "name": "item_2"},
                    ]
                )
            ),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items/1"),
            HttpResponse(body=json.dumps({"id": "1", "name": "item_1"})),
        )
        http_mocker.get(
            HttpRequest(url="https://api.test.com/items/2"),
            HttpResponse(body=json.dumps({"id": "2", "name": "item_2"})),
        )

        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
        )

        actual_catalog = source.discover(logger=source.logger, config=_CONFIG)

        configured_streams = [
            to_configured_stream(stream, primary_key=stream.source_defined_primary_key)
            for stream in actual_catalog.streams
        ]
        configured_catalog = to_configured_catalog(configured_streams)

        records = [
            message.record
            for message in source.read(MagicMock(), _CONFIG, configured_catalog)
            if message.type == Type.RECORD
        ]

    assert len(actual_catalog.streams) == 2
    assert set([stream.name for stream in actual_catalog.streams]) == expected_stream_names
    assert len(records) == 2
    assert set([record.stream for record in records]) == expected_stream_names


def test_duplicated_dynamic_streams_read_with_http_components_resolver():
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/duplicates_items"),
            HttpResponse(
                body=json.dumps(
                    [
                        {"id": 1, "name": "item_1"},
                        {"id": 2, "name": "item_2"},
                        {"id": 3, "name": "item_2"},
                    ]
                )
            ),
        )

        with pytest.raises(AirbyteTracedException) as exc_info:
            source = ConcurrentDeclarativeSource(
                source_config=_MANIFEST_WITH_DUPLICATES, config=_CONFIG, catalog=None, state=None
            )
            source.discover(logger=source.logger, config=_CONFIG)
        assert (
            str(exc_info.value)
            == "Dynamic streams list contains a duplicate name: item_2. Please contact Airbyte Support."
        )


def test_dynamic_streams_with_http_components_resolver_retriever_with_parent_stream():
    expected_stream_names = [
        "parent_1_item_1",
        "parent_1_item_2",
        "parent_2_item_1",
        "parent_2_item_2",
    ]
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(url="https://api.test.com/parents"),
            HttpResponse(body=json.dumps([{"id": 1}, {"id": 2}])),
        )
        parent_ids = [1, 2]
        for parent_id in parent_ids:
            http_mocker.get(
                HttpRequest(url=f"https://api.test.com/parent/{parent_id}/items"),
                HttpResponse(
                    body=json.dumps(
                        [
                            {"id": 1, "name": "item_1"},
                            {"id": 2, "name": "item_2"},
                        ]
                    )
                ),
            )
        dynamic_stream_paths = ["1/1", "2/1", "1/2", "2/2"]
        for dynamic_stream_path in dynamic_stream_paths:
            http_mocker.get(
                HttpRequest(url=f"https://api.test.com/{dynamic_stream_path}"),
                HttpResponse(body=json.dumps([{"ABC": 1, "AED": 2}])),
            )

        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST_WITH_HTTP_COMPONENT_RESOLVER_WITH_RETRIEVER_WITH_PARENT_STREAM,
            config=_CONFIG,
            catalog=None,
            state=None,
        )

        actual_catalog = source.discover(logger=source.logger, config=_CONFIG)

        configured_streams = [
            to_configured_stream(stream, primary_key=stream.source_defined_primary_key)
            for stream in actual_catalog.streams
        ]
        configured_catalog = to_configured_catalog(configured_streams)

        records = [
            message.record
            for message in source.read(MagicMock(), _CONFIG, configured_catalog)
            if message.type == Type.RECORD
        ]

    assert len(actual_catalog.streams) == 4
    assert [stream.name for stream in actual_catalog.streams] == expected_stream_names
    assert len(records) == 4

    actual_record_stream_names = [record.stream for record in records]
    actual_record_stream_names.sort()

    assert actual_record_stream_names == expected_stream_names
