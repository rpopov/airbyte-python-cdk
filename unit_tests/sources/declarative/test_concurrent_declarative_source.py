#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import copy
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union
from unittest.mock import patch

import freezegun
import isodate
import pendulum
from typing_extensions import deprecated

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    FailureType,
    Status,
    StreamDescriptor,
    SyncMode,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.sources.declarative.declarative_stream import DeclarativeStream
from airbyte_cdk.sources.declarative.partition_routers import AsyncJobPartitionRouter
from airbyte_cdk.sources.declarative.stream_slicers.declarative_partition_generator import (
    StreamSlicerPartitionGenerator,
)
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.checkpoint import Cursor
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor
from airbyte_cdk.sources.streams.concurrent.default_stream import DefaultStream
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import Record, StreamSlice
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.utils import AirbyteTracedException

_CONFIG = {"start_date": "2024-07-01T00:00:00.000Z"}

_CATALOG = ConfiguredAirbyteCatalog(
    streams=[
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="party_members", json_schema={}, supported_sync_modes=[SyncMode.incremental]
            ),
            sync_mode=SyncMode.incremental,
            destination_sync_mode=DestinationSyncMode.append,
        ),
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        ),
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="locations", json_schema={}, supported_sync_modes=[SyncMode.incremental]
            ),
            sync_mode=SyncMode.incremental,
            destination_sync_mode=DestinationSyncMode.append,
        ),
        ConfiguredAirbyteStream(
            stream=AirbyteStream(
                name="party_members_skills",
                json_schema={},
                supported_sync_modes=[SyncMode.full_refresh],
            ),
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
        ),
    ]
)
_LOCATIONS_RESPONSE = HttpResponse(
    json.dumps(
        [
            {"id": "444", "name": "Yongen-jaya", "updated_at": "2024-08-10"},
            {"id": "scramble", "name": "Shibuya", "updated_at": "2024-08-10"},
            {"id": "aoyama", "name": "Aoyama-itchome", "updated_at": "2024-08-10"},
            {"id": "shin123", "name": "Shinjuku", "updated_at": "2024-08-10"},
        ]
    )
)
_PALACES_RESPONSE = HttpResponse(
    json.dumps(
        [
            {"id": "0", "world": "castle", "owner": "kamoshida"},
            {"id": "1", "world": "museum", "owner": "madarame"},
            {"id": "2", "world": "bank", "owner": "kaneshiro"},
            {"id": "3", "world": "pyramid", "owner": "futaba"},
            {"id": "4", "world": "spaceport", "owner": "okumura"},
            {"id": "5", "world": "casino", "owner": "nijima"},
            {"id": "6", "world": "cruiser", "owner": "shido"},
        ]
    )
)
_PARTY_MEMBERS_SKILLS_RESPONSE = HttpResponse(
    json.dumps(
        [
            {"id": "0", "name": "hassou tobi"},
            {"id": "1", "name": "mafreidyne"},
            {"id": "2", "name": "myriad truths"},
        ]
    )
)
_EMPTY_RESPONSE = HttpResponse(json.dumps([]))
_NOW = "2024-09-10T00:00:00"
_NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES = [
    (
        {"start": "2024-07-01", "end": "2024-07-15"},
        HttpResponse(
            json.dumps(
                [
                    {
                        "id": "amamiya",
                        "first_name": "ren",
                        "last_name": "amamiya",
                        "updated_at": "2024-07-10",
                    }
                ]
            )
        ),
    ),
    ({"start": "2024-07-16", "end": "2024-07-30"}, _EMPTY_RESPONSE),
    (
        {"start": "2024-07-31", "end": "2024-08-14"},
        HttpResponse(
            json.dumps(
                [
                    {
                        "id": "nijima",
                        "first_name": "makoto",
                        "last_name": "nijima",
                        "updated_at": "2024-08-10",
                    },
                ]
            )
        ),
    ),
    ({"start": "2024-08-15", "end": "2024-08-29"}, _EMPTY_RESPONSE),
    (
        {"start": "2024-08-30", "end": "2024-09-10"},
        HttpResponse(
            json.dumps(
                [
                    {
                        "id": "yoshizawa",
                        "first_name": "sumire",
                        "last_name": "yoshizawa",
                        "updated_at": "2024-09-10",
                    }
                ]
            )
        ),
    ),
]
_MANIFEST = {
    "version": "5.0.0",
    "definitions": {
        "selector": {
            "type": "RecordSelector",
            "extractor": {"type": "DpathExtractor", "field_path": []},
        },
        "requester": {
            "type": "HttpRequester",
            "url_base": "https://persona.metaverse.com",
            "http_method": "GET",
            "authenticator": {
                "type": "BasicHttpAuthenticator",
                "username": "{{ config['api_key'] }}",
                "password": "{{ config['secret_key'] }}",
            },
            "error_handler": {
                "type": "DefaultErrorHandler",
                "response_filters": [
                    {
                        "http_codes": [403],
                        "action": "FAIL",
                        "failure_type": "config_error",
                        "error_message": "Access denied due to lack of permission or invalid API/Secret key or wrong data region.",
                    },
                    {
                        "http_codes": [404],
                        "action": "IGNORE",
                        "error_message": "No data available for the time range requested.",
                    },
                ],
            },
        },
        "retriever": {
            "type": "SimpleRetriever",
            "record_selector": {"$ref": "#/definitions/selector"},
            "paginator": {"type": "NoPagination"},
            "requester": {"$ref": "#/definitions/requester"},
        },
        "incremental_cursor": {
            "type": "DatetimeBasedCursor",
            "start_datetime": {
                "datetime": "{{ format_datetime(config['start_date'], '%Y-%m-%d') }}"
            },
            "end_datetime": {"datetime": "{{ now_utc().strftime('%Y-%m-%d') }}"},
            "datetime_format": "%Y-%m-%d",
            "cursor_datetime_formats": ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
            "cursor_granularity": "P1D",
            "step": "P15D",
            "cursor_field": "updated_at",
            "lookback_window": "P5D",
            "start_time_option": {
                "type": "RequestOption",
                "field_name": "start",
                "inject_into": "request_parameter",
            },
            "end_time_option": {
                "type": "RequestOption",
                "field_name": "end",
                "inject_into": "request_parameter",
            },
        },
        "base_stream": {"retriever": {"$ref": "#/definitions/retriever"}},
        "base_incremental_stream": {
            "retriever": {
                "$ref": "#/definitions/retriever",
                "requester": {"$ref": "#/definitions/requester"},
            },
            "incremental_sync": {"$ref": "#/definitions/incremental_cursor"},
        },
        "party_members_stream": {
            "$ref": "#/definitions/base_incremental_stream",
            "retriever": {
                "$ref": "#/definitions/base_incremental_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
            },
            "$parameters": {"name": "party_members", "primary_key": "id", "path": "/party_members"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the party member",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "palaces_stream": {
            "$ref": "#/definitions/base_stream",
            "$parameters": {"name": "palaces", "primary_key": "id", "path": "/palaces"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the metaverse palace",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "async_job_stream": {
            "$ref": "#/definitions/base_stream",
            "$parameters": {
                "name": "async_job_stream",
                "primary_key": "id",
                "url_base": "https://persona.metaverse.com",
            },
            "retriever": {
                "type": "AsyncRetriever",
                "status_mapping": {
                    "failed": ["failed"],
                    "running": ["pending"],
                    "timeout": ["timeout"],
                    "completed": ["ready"],
                },
                "urls_extractor": {"type": "DpathExtractor", "field_path": ["urls"]},
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
                "status_extractor": {"type": "DpathExtractor", "field_path": ["status"]},
                "polling_requester": {
                    "type": "HttpRequester",
                    "path": "/async_job/{{stream_slice['create_job_response'].json()['id'] }}",
                    "http_method": "GET",
                    "authenticator": {
                        "type": "BearerAuthenticator",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "creation_requester": {
                    "type": "HttpRequester",
                    "path": "async_job",
                    "http_method": "POST",
                    "authenticator": {
                        "type": "BearerAuthenticator",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "download_requester": {
                    "type": "HttpRequester",
                    "path": "{{stream_slice['url']}}",
                    "http_method": "GET",
                },
            },
            "incremental_sync": {"$ref": "#/definitions/incremental_cursor"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the metaverse palace",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "locations_stream": {
            "$ref": "#/definitions/base_incremental_stream",
            "retriever": {
                "$ref": "#/definitions/base_incremental_stream/retriever",
                "requester": {
                    "$ref": "#/definitions/base_incremental_stream/retriever/requester",
                    "request_parameters": {"m": "active", "i": "1", "g": "country"},
                },
                "record_selector": {"$ref": "#/definitions/selector"},
            },
            "incremental_sync": {
                "$ref": "#/definitions/incremental_cursor",
                "step": "P1M",
                "cursor_field": "updated_at",
            },
            "$parameters": {"name": "locations", "primary_key": "id", "path": "/locations"},
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the neighborhood location",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "party_members_skills_stream": {
            "$ref": "#/definitions/base_stream",
            "retriever": {
                "$ref": "#/definitions/base_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "type": "ParentStreamConfig",
                            "stream": "#/definitions/party_members_stream",
                            "parent_key": "id",
                            "partition_field": "party_member_id",
                        }
                    ],
                },
            },
            "$parameters": {
                "name": "party_members_skills",
                "primary_key": "id",
                "path": "/party_members/{{stream_slice.party_member_id}}/skills",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the party member",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "arcana_personas_stream": {
            "$ref": "#/definitions/base_stream",
            "retriever": {
                "$ref": "#/definitions/base_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "partition_router": {
                    "type": "ListPartitionRouter",
                    "cursor_field": "arcana_id",
                    "values": [
                        "Fool",
                        "Magician",
                        "Priestess",
                        "Empress",
                        "Emperor",
                        "Hierophant",
                        "Lovers",
                        "Chariot",
                        "Justice",
                        "Hermit",
                        "Fortune",
                        "Strength",
                        "Hanged Man",
                        "Death",
                        "Temperance",
                        "Devil",
                        "Tower",
                        "Star",
                        "Moon",
                        "Sun",
                        "Judgement",
                        "World",
                    ],
                },
            },
            "$parameters": {
                "name": "arcana_personas",
                "primary_key": "id",
                "path": "/arcanas/{{stream_slice.arcana_id}}/personas",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the persona",
                            "type": ["null", "string"],
                        },
                        "arcana_id": {
                            "description": "The associated arcana tarot for this persona",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
        "palace_enemies_stream": {
            "$ref": "#/definitions/base_incremental_stream",
            "retriever": {
                "$ref": "#/definitions/base_incremental_stream/retriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {
                            "type": "ParentStreamConfig",
                            "stream": "#/definitions/palaces_stream",
                            "parent_key": "id",
                            "partition_field": "palace_id",
                        }
                    ],
                },
            },
            "$parameters": {
                "name": "palace_enemies",
                "primary_key": "id",
                "path": "/palaces/{{stream_slice.palace_id}}/enemies",
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "$schema": "https://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The identifier",
                            "type": ["null", "string"],
                        },
                        "name": {
                            "description": "The name of the enemy persona",
                            "type": ["null", "string"],
                        },
                        "palace_id": {
                            "description": "The palace id where this persona exists in",
                            "type": ["null", "string"],
                        },
                    },
                },
            },
        },
    },
    "streams": [
        "#/definitions/party_members_stream",
        "#/definitions/palaces_stream",
        "#/definitions/locations_stream",
        "#/definitions/party_members_skills_stream",
        "#/definitions/arcana_personas_stream",
        "#/definitions/palace_enemies_stream",
        "#/definitions/async_job_stream",
    ],
    "check": {"stream_names": ["party_members", "locations"]},
    "concurrency_level": {
        "type": "ConcurrencyLevel",
        "default_concurrency": "{{ config['num_workers'] or 10 }}",
        "max_concurrency": 25,
    },
}


@deprecated("See note in docstring for more information")
class DeclarativeStreamDecorator(Stream):
    """
    Helper class that wraps an existing DeclarativeStream but allows for overriding the output of read_records() to
    make it easier to mock behavior and test how low-code streams integrate with the Concurrent CDK.

    NOTE: We are not using that for now but the intent was to scope the tests to only testing that streams were properly instantiated and
    interacted together properly. However, in practice, we had a couple surprises like `get_cursor` and `stream_slices` needed to be
    re-implemented as well. Because of that, we've move away from that in favour of doing tests that integrate up until the HTTP request.
    The drawback of that is that we are dependent on any change later (like if the DatetimeBasedCursor changes, this will affect those
    tests) but it feels less flaky than this. If we have new information in the future to infirm that, feel free to re-use this class as
    necessary.
    """

    def __init__(
        self,
        declarative_stream: DeclarativeStream,
        slice_to_records_mapping: Mapping[tuple[str, str], List[Mapping[str, Any]]],
    ):
        self._declarative_stream = declarative_stream
        self._slice_to_records_mapping = slice_to_records_mapping

    @property
    def name(self) -> str:
        return self._declarative_stream.name

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._declarative_stream.primary_key

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        if isinstance(stream_slice, StreamSlice):
            slice_key = (stream_slice.get("start_time"), stream_slice.get("end_time"))

            # Extra logic to simulate raising an error during certain partitions to validate error handling
            if slice_key == ("2024-08-05", "2024-09-04"):
                raise AirbyteTracedException(
                    message=f"Received an unexpected error during interval with start: {slice_key[0]} and end: {slice_key[1]}.",
                    failure_type=FailureType.config_error,
                )

            if slice_key in self._slice_to_records_mapping:
                yield from self._slice_to_records_mapping.get(slice_key)
            else:
                yield from []
        else:
            raise ValueError(
                f"stream_slice should be of type StreamSlice, but received {type(stream_slice)}"
            )

    def get_json_schema(self) -> Mapping[str, Any]:
        return self._declarative_stream.get_json_schema()

    def get_cursor(self) -> Optional[Cursor]:
        return self._declarative_stream.get_cursor()


def test_group_streams():
    """
    Tests the grouping of low-code streams into ones that can be processed concurrently vs ones that must be processed concurrently
    """

    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="party_members",
                    json_schema={},
                    supported_sync_modes=[SyncMode.incremental],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="locations", json_schema={}, supported_sync_modes=[SyncMode.incremental]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="party_members_skills",
                    json_schema={},
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    state = []

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=catalog, state=state
    )
    concurrent_streams, synchronous_streams = source._group_streams(config=_CONFIG)

    # 1 full refresh stream, 2 incremental streams, 1 substream w/o incremental, 1 list based substream w/o incremental
    # 1 async job stream, 1 substream w/ incremental
    assert len(concurrent_streams) == 7
    (
        concurrent_stream_0,
        concurrent_stream_1,
        concurrent_stream_2,
        concurrent_stream_3,
        concurrent_stream_4,
        concurrent_stream_5,
        concurrent_stream_6,
    ) = concurrent_streams
    assert isinstance(concurrent_stream_0, DefaultStream)
    assert concurrent_stream_0.name == "party_members"
    assert isinstance(concurrent_stream_1, DefaultStream)
    assert concurrent_stream_1.name == "palaces"
    assert isinstance(concurrent_stream_2, DefaultStream)
    assert concurrent_stream_2.name == "locations"
    assert isinstance(concurrent_stream_3, DefaultStream)
    assert concurrent_stream_3.name == "party_members_skills"
    assert isinstance(concurrent_stream_4, DefaultStream)
    assert concurrent_stream_4.name == "arcana_personas"
    assert isinstance(concurrent_stream_5, DefaultStream)
    assert concurrent_stream_5.name == "palace_enemies"
    assert isinstance(concurrent_stream_6, DefaultStream)
    assert concurrent_stream_6.name == "async_job_stream"


@freezegun.freeze_time(time_to_freeze=datetime(2024, 9, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
def test_create_concurrent_cursor():
    """
    Validate that the ConcurrentDeclarativeSource properly instantiates a ConcurrentCursor from the
    low-code DatetimeBasedCursor component
    """

    incoming_locations_state = {
        "slices": [
            {"start": "2024-07-01T00:00:00", "end": "2024-07-31T00:00:00"},
        ],
        "state_type": "date-range",
    }

    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(**incoming_locations_state),
            ),
        ),
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )
    concurrent_streams, synchronous_streams = source._group_streams(config=_CONFIG)

    party_members_stream = concurrent_streams[0]
    assert isinstance(party_members_stream, DefaultStream)
    party_members_cursor = party_members_stream.cursor

    assert isinstance(party_members_cursor, ConcurrentCursor)
    assert party_members_cursor._stream_name == "party_members"
    assert party_members_cursor._cursor_field.cursor_field_key == "updated_at"
    assert party_members_cursor._start == pendulum.parse(_CONFIG.get("start_date"))
    assert party_members_cursor._end_provider() == datetime(
        year=2024, month=9, day=1, tzinfo=timezone.utc
    )
    assert party_members_cursor._slice_boundary_fields == ("start_time", "end_time")
    assert party_members_cursor._slice_range == timedelta(days=15)
    assert party_members_cursor._lookback_window == timedelta(days=5)
    assert party_members_cursor._cursor_granularity == timedelta(days=1)

    locations_stream = concurrent_streams[2]
    assert isinstance(locations_stream, DefaultStream)
    locations_cursor = locations_stream.cursor

    assert isinstance(locations_cursor, ConcurrentCursor)
    assert locations_cursor._stream_name == "locations"
    assert locations_cursor._cursor_field.cursor_field_key == "updated_at"
    assert locations_cursor._start == pendulum.parse(_CONFIG.get("start_date"))
    assert locations_cursor._end_provider() == datetime(
        year=2024, month=9, day=1, tzinfo=timezone.utc
    )
    assert locations_cursor._slice_boundary_fields == ("start_time", "end_time")
    assert locations_cursor._slice_range == isodate.Duration(months=1)
    assert locations_cursor._lookback_window == timedelta(days=5)
    assert locations_cursor._cursor_granularity == timedelta(days=1)
    assert locations_cursor._concurrent_state == {
        "slices": [
            {
                "start": datetime(2024, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
                "end": datetime(2024, 7, 31, 0, 0, 0, 0, tzinfo=timezone.utc),
            }
        ],
        "state_type": "date-range",
    }


def test_check():
    """
    Verifies that the ConcurrentDeclarativeSource check command is run against synchronous streams
    """
    with HttpMocker() as http_mocker:
        http_mocker.get(
            HttpRequest(
                "https://persona.metaverse.com/party_members?start=2024-07-01&end=2024-07-15"
            ),
            HttpResponse(
                json.dumps(
                    {
                        "id": "amamiya",
                        "first_name": "ren",
                        "last_name": "amamiya",
                        "updated_at": "2024-07-10",
                    }
                )
            ),
        )
        http_mocker.get(
            HttpRequest("https://persona.metaverse.com/palaces"),
            HttpResponse(json.dumps({"id": "palace_1"})),
        )
        http_mocker.get(
            HttpRequest(
                "https://persona.metaverse.com/locations?m=active&i=1&g=country&start=2024-07-01&end=2024-07-31"
            ),
            HttpResponse(json.dumps({"id": "location_1"})),
        )
        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
        )

        connection_status = source.check(logger=source.logger, config=_CONFIG)

    assert connection_status.status == Status.SUCCEEDED


def test_discover():
    """
    Verifies that the ConcurrentDeclarativeSource discover command returns concurrent and synchronous catalog definitions
    """
    expected_stream_names = {
        "party_members",
        "palaces",
        "locations",
        "party_members_skills",
        "arcana_personas",
        "palace_enemies",
        "async_job_stream",
    }

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=None, state=None
    )

    actual_catalog = source.discover(logger=source.logger, config=_CONFIG)

    assert set(map(lambda stream: stream.name, actual_catalog.streams)) == expected_stream_names


def _mock_requests(
    http_mocker: HttpMocker,
    url: str,
    query_params: List[Dict[str, str]],
    responses: List[HttpResponse],
) -> None:
    assert len(query_params) == len(responses), "Expecting as many slices as response"

    for i in range(len(query_params)):
        http_mocker.get(HttpRequest(url, query_params=query_params[i]), responses[i])


def _mock_party_members_requests(
    http_mocker: HttpMocker, slices_and_responses: List[Tuple[Dict[str, str], HttpResponse]]
) -> None:
    slices = list(map(lambda slice_and_response: slice_and_response[0], slices_and_responses))
    responses = list(map(lambda slice_and_response: slice_and_response[1], slices_and_responses))

    _mock_requests(
        http_mocker,
        "https://persona.metaverse.com/party_members",
        slices,
        responses,
    )


def _mock_locations_requests(http_mocker: HttpMocker, slices: List[Dict[str, str]]) -> None:
    locations_query_params = list(
        map(lambda _slice: _slice | {"m": "active", "i": "1", "g": "country"}, slices)
    )
    _mock_requests(
        http_mocker,
        "https://persona.metaverse.com/locations",
        locations_query_params,
        [_LOCATIONS_RESPONSE] * len(slices),
    )


def _mock_party_members_skills_requests(http_mocker: HttpMocker) -> None:
    """
    This method assumes _mock_party_members_requests has been called before else the stream won't work.
    """
    http_mocker.get(
        HttpRequest("https://persona.metaverse.com/party_members/amamiya/skills"),
        _PARTY_MEMBERS_SKILLS_RESPONSE,
    )
    http_mocker.get(
        HttpRequest("https://persona.metaverse.com/party_members/nijima/skills"),
        _PARTY_MEMBERS_SKILLS_RESPONSE,
    )
    http_mocker.get(
        HttpRequest("https://persona.metaverse.com/party_members/yoshizawa/skills"),
        _PARTY_MEMBERS_SKILLS_RESPONSE,
    )


def mocked_init(self, is_sequential_state: bool = True):
    """
    This method is used to patch the existing __init__() function and always set is_sequential_state to
    false. This is required because we want to test the concurrent state format. And because streams are
    created under the hood of the read/discover/check command, we have no way of setting the field without
    patching __init__()
    """
    self._is_sequential_state = False


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_with_concurrent_and_synchronous_streams():
    """
    Verifies that a ConcurrentDeclarativeSource processes concurrent streams followed by synchronous streams
    """
    location_slices = [
        {"start": "2024-07-01", "end": "2024-07-31"},
        {"start": "2024-08-01", "end": "2024-08-31"},
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]
    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=None
    )

    with HttpMocker() as http_mocker:
        _mock_party_members_requests(http_mocker, _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES)
        _mock_locations_requests(http_mocker, location_slices)
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)
        _mock_party_members_skills_requests(http_mocker)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=_CATALOG, state=[])
        )

    # See _mock_party_members_requests
    party_members_records = get_records_for_stream("party_members", messages)
    assert len(party_members_records) == 3

    party_members_states = get_states_for_stream(stream_name="party_members", messages=messages)
    assert len(party_members_states) == 6
    assert (
        party_members_states[5].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-09-10",
                }
            ],
        ).__dict__
    )

    # Expects 12 records, 3 slices, 4 records each slice
    locations_records = get_records_for_stream(stream_name="locations", messages=messages)
    assert len(locations_records) == 12

    # 3 partitions == 3 state messages + final state message
    # Because we cannot guarantee the order partitions finish, we only validate that the final state has the latest checkpoint value
    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 4
    assert (
        locations_states[3].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-08-10",
                }
            ],
        ).__dict__
    )

    # Expects 7 records, 1 empty slice, 7 records in slice
    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7

    palaces_states = get_states_for_stream(stream_name="palaces", messages=messages)
    assert len(palaces_states) == 1
    assert (
        palaces_states[0].stream.stream_state.__dict__
        == AirbyteStateBlob(__ab_no_cursor_state_message=True).__dict__
    )

    # Expects 3 records, 3 slices, 3 records in slice
    party_members_skills_records = get_records_for_stream("party_members_skills", messages)
    assert len(party_members_skills_records) == 9

    party_members_skills_states = get_states_for_stream(
        stream_name="party_members_skills", messages=messages
    )
    assert len(party_members_skills_states) == 1
    assert (
        party_members_skills_states[0].stream.stream_state.__dict__
        == AirbyteStateBlob(__ab_no_cursor_state_message=True).__dict__
    )


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_with_concurrent_and_synchronous_streams_with_concurrent_state():
    """
    Verifies that a ConcurrentDeclarativeSource processes concurrent streams correctly using the incoming
    concurrent state format
    """
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(
                    state_type="date-range",
                    slices=[{"start": "2024-07-01", "end": "2024-07-31"}],
                ),
            ),
        ),
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="party_members", namespace=None),
                stream_state=AirbyteStateBlob(
                    state_type="date-range",
                    slices=[
                        {"start": "2024-07-16", "end": "2024-07-30"},
                        {"start": "2024-07-31", "end": "2024-08-14"},
                        {"start": "2024-08-30", "end": "2024-09-09"},
                    ],
                ),
            ),
        ),
    ]

    party_members_slices_and_responses = _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES + [
        (
            {"start": "2024-09-04", "end": "2024-09-10"},  # considering lookback window
            HttpResponse(
                json.dumps(
                    [
                        {
                            "id": "yoshizawa",
                            "first_name": "sumire",
                            "last_name": "yoshizawa",
                            "updated_at": "2024-09-10",
                        }
                    ]
                )
            ),
        )
    ]
    location_slices = [
        {"start": "2024-07-26", "end": "2024-08-25"},
        {"start": "2024-08-26", "end": "2024-09-10"},
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )

    with HttpMocker() as http_mocker:
        _mock_party_members_requests(http_mocker, party_members_slices_and_responses)
        _mock_locations_requests(http_mocker, location_slices)
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)
        _mock_party_members_skills_requests(http_mocker)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=_CATALOG, state=state)
        )

    # Expects 8 records, skip successful intervals and are left with 2 slices, 4 records each slice
    locations_records = get_records_for_stream("locations", messages)
    assert len(locations_records) == 8

    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 3
    assert (
        locations_states[2].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-08-10",
                }
            ],
        ).__dict__
    )

    # slices to sync are:
    # * {"start": "2024-07-01", "end": "2024-07-15"}: one record in _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES
    # * {"start": "2024-09-04", "end": "2024-09-10"}: one record from the lookback window defined in this test
    party_members_records = get_records_for_stream("party_members", messages)
    assert len(party_members_records) == 2

    party_members_states = get_states_for_stream(stream_name="party_members", messages=messages)
    assert len(party_members_states) == 4
    assert (
        party_members_states[3].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-09-10",
                }
            ],
        ).__dict__
    )

    # Expects 7 records, 1 empty slice, 7 records in slice
    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7

    # Expects 3 records, 3 slices, 3 records in slice
    party_members_skills_records = get_records_for_stream("party_members_skills", messages)
    assert len(party_members_skills_records) == 9


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_with_concurrent_and_synchronous_streams_with_sequential_state():
    """
    Verifies that a ConcurrentDeclarativeSource processes concurrent streams correctly using the incoming
    legacy state format
    """
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="locations", namespace=None),
                stream_state=AirbyteStateBlob(updated_at="2024-08-06"),
            ),
        ),
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="party_members", namespace=None),
                stream_state=AirbyteStateBlob(updated_at="2024-08-21"),
            ),
        ),
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )

    party_members_slices_and_responses = _NO_STATE_PARTY_MEMBERS_SLICES_AND_RESPONSES + [
        (
            {"start": "2024-08-16", "end": "2024-08-30"},
            HttpResponse(
                json.dumps(
                    [
                        {
                            "id": "nijima",
                            "first_name": "makoto",
                            "last_name": "nijima",
                            "updated_at": "2024-08-10",
                        }
                    ]
                )
            ),
        ),  # considering lookback window
        (
            {"start": "2024-08-31", "end": "2024-09-10"},
            HttpResponse(
                json.dumps(
                    [
                        {
                            "id": "yoshizawa",
                            "first_name": "sumire",
                            "last_name": "yoshizawa",
                            "updated_at": "2024-09-10",
                        }
                    ]
                )
            ),
        ),
    ]
    location_slices = [
        {"start": "2024-08-01", "end": "2024-08-31"},
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]

    with HttpMocker() as http_mocker:
        _mock_party_members_requests(http_mocker, party_members_slices_and_responses)
        _mock_locations_requests(http_mocker, location_slices)
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)
        _mock_party_members_skills_requests(http_mocker)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=_CATALOG, state=state)
        )

    # Expects 8 records, skip successful intervals and are left with 2 slices, 4 records each slice
    locations_records = get_records_for_stream("locations", messages)
    assert len(locations_records) == 8

    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 3
    assert (
        locations_states[2].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-08-10",
                }
            ],
        ).__dict__
    )

    # From extra slices defined in party_members_slices_and_responses
    party_members_records = get_records_for_stream("party_members", messages)
    assert len(party_members_records) == 2

    party_members_states = get_states_for_stream(stream_name="party_members", messages=messages)
    assert len(party_members_states) == 3
    assert (
        party_members_states[2].stream.stream_state.__dict__
        == AirbyteStateBlob(
            state_type="date-range",
            slices=[
                {
                    "start": "2024-07-01",
                    "end": "2024-09-10",
                    "most_recent_cursor_value": "2024-09-10",
                }
            ],
        ).__dict__
    )

    # Expects 7 records, 1 empty slice, 7 records in slice
    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7

    # Expects 3 records, 3 slices, 3 records in slice
    party_members_skills_records = get_records_for_stream("party_members_skills", messages)
    assert len(party_members_skills_records) == 9


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_concurrent_with_failing_partition_in_the_middle():
    """
    Verify that partial state is emitted when only some partitions are successful during a concurrent sync attempt
    """
    location_slices = [
        {"start": "2024-07-01", "end": "2024-07-31"},
        # missing slice `{"start": "2024-08-01", "end": "2024-08-31"}` here
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]
    expected_stream_state = {
        "state_type": "date-range",
        "slices": [
            location_slice | {"most_recent_cursor_value": "2024-08-10"}
            for location_slice in location_slices
        ],
    }

    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="locations", json_schema={}, supported_sync_modes=[SyncMode.incremental]
                ),
                sync_mode=SyncMode.incremental,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=catalog, state=[]
    )

    location_slices = [
        {"start": "2024-07-01", "end": "2024-07-31"},
        # missing slice `{"start": "2024-08-01", "end": "2024-08-31"}` here
        {"start": "2024-09-01", "end": "2024-09-10"},
    ]

    with HttpMocker() as http_mocker:
        _mock_locations_requests(http_mocker, location_slices)

        messages = []
        try:
            for message in source.read(
                logger=source.logger, config=_CONFIG, catalog=catalog, state=[]
            ):
                messages.append(message)
        except AirbyteTracedException:
            assert (
                get_states_for_stream(stream_name="locations", messages=messages)[
                    -1
                ].stream.stream_state.__dict__
                == expected_stream_state
            )


@freezegun.freeze_time(_NOW)
@patch(
    "airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter.AbstractStreamStateConverter.__init__",
    mocked_init,
)
def test_read_concurrent_skip_streams_not_in_catalog():
    """
    Verifies that the ConcurrentDeclarativeSource only syncs streams that are specified in the incoming ConfiguredCatalog
    """
    with HttpMocker() as http_mocker:
        catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                    ),
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.append,
                ),
                ConfiguredAirbyteStream(
                    stream=AirbyteStream(
                        name="locations",
                        json_schema={},
                        supported_sync_modes=[SyncMode.incremental],
                    ),
                    sync_mode=SyncMode.incremental,
                    destination_sync_mode=DestinationSyncMode.append,
                ),
            ]
        )

        source = ConcurrentDeclarativeSource(
            source_config=_MANIFEST, config=_CONFIG, catalog=catalog, state=None
        )
        # locations requests
        location_slices = [
            {"start": "2024-07-01", "end": "2024-07-31"},
            {"start": "2024-08-01", "end": "2024-08-31"},
            {"start": "2024-09-01", "end": "2024-09-10"},
        ]
        locations_query_params = list(
            map(lambda _slice: _slice | {"m": "active", "i": "1", "g": "country"}, location_slices)
        )
        _mock_requests(
            http_mocker,
            "https://persona.metaverse.com/locations",
            locations_query_params,
            [_LOCATIONS_RESPONSE] * len(location_slices),
        )

        # palaces requests
        http_mocker.get(HttpRequest("https://persona.metaverse.com/palaces"), _PALACES_RESPONSE)

        messages = list(
            source.read(logger=source.logger, config=_CONFIG, catalog=catalog, state=[])
        )

    locations_records = get_records_for_stream(stream_name="locations", messages=messages)
    assert len(locations_records) == 12
    locations_states = get_states_for_stream(stream_name="locations", messages=messages)
    assert len(locations_states) == 4

    palaces_records = get_records_for_stream("palaces", messages)
    assert len(palaces_records) == 7
    palaces_states = get_states_for_stream(stream_name="palaces", messages=messages)
    assert len(palaces_states) == 1

    assert len(get_records_for_stream(stream_name="party_members", messages=messages)) == 0
    assert len(get_states_for_stream(stream_name="party_members", messages=messages)) == 0

    assert len(get_records_for_stream(stream_name="party_members_skills", messages=messages)) == 0
    assert len(get_states_for_stream(stream_name="party_members_skills", messages=messages)) == 0


def test_default_perform_interpolation_on_concurrency_level():
    config = {"start_date": "2024-07-01T00:00:00.000Z", "num_workers": 20}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=config, catalog=catalog, state=[]
    )
    assert (
        source._concurrent_source._initial_number_partitions_to_generate == 10
    )  # We floor the number of initial partitions on creation


def test_default_to_single_threaded_when_no_concurrency_level():
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    manifest = copy.deepcopy(_MANIFEST)
    del manifest["concurrency_level"]

    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=_CONFIG, catalog=catalog, state=[]
    )
    assert source._concurrent_source._initial_number_partitions_to_generate == 1


def test_concurrency_level_initial_number_partitions_to_generate_is_always_one_or_more():
    config = {"start_date": "2024-07-01T00:00:00.000Z", "num_workers": 1}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="palaces", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            ),
        ]
    )

    manifest = copy.deepcopy(_MANIFEST)
    manifest["concurrency_level"] = {
        "type": "ConcurrencyLevel",
        "default_concurrency": "{{ config.get('num_workers', 1) }}",
        "max_concurrency": 25,
    }

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=config, catalog=catalog, state=[]
    )
    assert source._concurrent_source._initial_number_partitions_to_generate == 1


def test_streams_with_stream_state_interpolation_should_be_synchronous():
    manifest_with_stream_state_interpolation = copy.deepcopy(_MANIFEST)

    # Add stream_state interpolation to the location stream's HttpRequester
    manifest_with_stream_state_interpolation["definitions"]["locations_stream"]["retriever"][
        "requester"
    ]["request_parameters"] = {
        "after": "{{ stream_state['updated_at'] }}",
    }

    # Add a RecordFilter component that uses stream_state interpolation to the party member stream
    manifest_with_stream_state_interpolation["definitions"]["party_members_stream"]["retriever"][
        "record_selector"
    ]["record_filter"] = {
        "type": "RecordFilter",
        "condition": "{{ record.updated_at > stream_state['updated_at'] }}",
    }

    source = ConcurrentDeclarativeSource(
        source_config=manifest_with_stream_state_interpolation,
        config=_CONFIG,
        catalog=_CATALOG,
        state=None,
    )
    concurrent_streams, synchronous_streams = source._group_streams(config=_CONFIG)

    # 1 full refresh stream, 2 with parent stream without incremental dependency, 1 stream with async retriever, 1 incremental with parent stream (palace_enemies)
    assert len(concurrent_streams) == 5
    # 2 incremental stream with interpolation on state (locations and party_members)
    assert len(synchronous_streams) == 2


def test_given_partition_routing_and_incremental_sync_then_stream_is_concurrent():
    manifest = {
        "version": "5.0.0",
        "definitions": {
            "selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://persona.metaverse.com",
                "http_method": "GET",
                "authenticator": {
                    "type": "BasicHttpAuthenticator",
                    "username": "{{ config['api_key'] }}",
                    "password": "{{ config['secret_key'] }}",
                },
                "error_handler": {
                    "type": "DefaultErrorHandler",
                    "response_filters": [
                        {
                            "http_codes": [403],
                            "action": "FAIL",
                            "failure_type": "config_error",
                            "error_message": "Access denied due to lack of permission or invalid API/Secret key or wrong data region.",
                        },
                        {
                            "http_codes": [404],
                            "action": "IGNORE",
                            "error_message": "No data available for the time range requested.",
                        },
                    ],
                },
            },
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {"$ref": "#/definitions/selector"},
                "paginator": {"type": "NoPagination"},
                "requester": {"$ref": "#/definitions/requester"},
            },
            "incremental_cursor": {
                "type": "DatetimeBasedCursor",
                "start_datetime": {
                    "datetime": "{{ format_datetime(config['start_date'], '%Y-%m-%d') }}"
                },
                "end_datetime": {"datetime": "{{ now_utc().strftime('%Y-%m-%d') }}"},
                "datetime_format": "%Y-%m-%d",
                "cursor_datetime_formats": ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
                "cursor_granularity": "P1D",
                "step": "P15D",
                "cursor_field": "updated_at",
                "lookback_window": "P5D",
                "start_time_option": {
                    "type": "RequestOption",
                    "field_name": "start",
                    "inject_into": "request_parameter",
                },
                "end_time_option": {
                    "type": "RequestOption",
                    "field_name": "end",
                    "inject_into": "request_parameter",
                },
            },
            "base_stream": {"retriever": {"$ref": "#/definitions/retriever"}},
            "base_incremental_stream": {
                "retriever": {
                    "$ref": "#/definitions/retriever",
                    "requester": {"$ref": "#/definitions/requester"},
                },
                "incremental_sync": {"$ref": "#/definitions/incremental_cursor"},
            },
            "incremental_party_members_skills_stream": {
                "$ref": "#/definitions/base_incremental_stream",
                "retriever": {
                    "$ref": "#/definitions/base_incremental_stream/retriever",
                    "partition_router": {
                        "type": "ListPartitionRouter",
                        "cursor_field": "party_member_id",
                        "values": ["party_member1", "party_member2"],
                    },
                },
                "$parameters": {
                    "name": "incremental_party_members_skills",
                    "primary_key": "id",
                    "path": "/party_members/{{stream_slice.party_member_id}}/skills",
                },
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "https://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "properties": {
                            "id": {
                                "description": "The identifier",
                                "type": ["null", "string"],
                            },
                            "name": {
                                "description": "The name of the party member",
                                "type": ["null", "string"],
                            },
                        },
                    },
                },
            },
        },
        "streams": ["#/definitions/incremental_party_members_skills_stream"],
        "check": {"stream_names": ["incremental_party_members_skills"]},
        "concurrency_level": {
            "type": "ConcurrencyLevel",
            "default_concurrency": "{{ config['num_workers'] or 10 }}",
            "max_concurrency": 25,
        },
    }

    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="incremental_party_members_skills",
                    json_schema={},
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.incremental,
                destination_sync_mode=DestinationSyncMode.append,
            )
        ]
    )

    state = []

    source = ConcurrentDeclarativeSource(
        source_config=manifest, config=_CONFIG, catalog=catalog, state=state
    )
    concurrent_streams, synchronous_streams = source._group_streams(config=_CONFIG)

    assert len(concurrent_streams) == 1
    assert len(synchronous_streams) == 0


def test_async_incremental_stream_uses_concurrent_cursor_with_state():
    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="async_job_stream", namespace=None),
                stream_state=AirbyteStateBlob(updated_at="2024-08-06"),
            ),
        )
    ]

    source = ConcurrentDeclarativeSource(
        source_config=_MANIFEST, config=_CONFIG, catalog=_CATALOG, state=state
    )

    expected_state = {
        "legacy": {"updated_at": "2024-08-06"},
        "slices": [
            {
                "end": datetime(2024, 8, 6, 0, 0, tzinfo=timezone.utc),
                "most_recent_cursor_value": datetime(2024, 8, 6, 0, 0, tzinfo=timezone.utc),
                "start": datetime(2024, 7, 1, 0, 0, tzinfo=timezone.utc),
            }
        ],
        "state_type": "date-range",
    }

    concurrent_streams, _ = source._group_streams(config=_CONFIG)
    async_job_stream = concurrent_streams[6]
    assert isinstance(async_job_stream, DefaultStream)
    cursor = async_job_stream._cursor
    assert isinstance(cursor, ConcurrentCursor)
    assert cursor._concurrent_state == expected_state
    stream_partition_generator = async_job_stream._stream_partition_generator
    assert isinstance(stream_partition_generator, StreamSlicerPartitionGenerator)
    async_job_partition_router = stream_partition_generator._stream_slicer
    assert isinstance(async_job_partition_router, AsyncJobPartitionRouter)
    assert isinstance(async_job_partition_router.stream_slicer, ConcurrentCursor)
    assert async_job_partition_router.stream_slicer._concurrent_state == expected_state


def create_wrapped_stream(stream: DeclarativeStream) -> Stream:
    slice_to_records_mapping = get_mocked_read_records_output(stream_name=stream.name)

    return DeclarativeStreamDecorator(
        declarative_stream=stream, slice_to_records_mapping=slice_to_records_mapping
    )


def get_mocked_read_records_output(stream_name: str) -> Mapping[tuple[str, str], List[StreamData]]:
    match stream_name:
        case "locations":
            slices = [
                # Slices used during first incremental sync
                StreamSlice(
                    cursor_slice={"start": "2024-07-01", "end": "2024-07-31"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-01", "end": "2024-08-31"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-09-01", "end": "2024-09-09"}, partition={}
                ),
                # Slices used during incremental checkpoint sync
                StreamSlice(
                    cursor_slice={"start": "2024-07-26", "end": "2024-08-25"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-26", "end": "2024-09-09"}, partition={}
                ),
                # Slices used during incremental sync with some partitions that exit with an error
                StreamSlice(
                    cursor_slice={"start": "2024-07-05", "end": "2024-08-04"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-05", "end": "2024-09-04"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-09-05", "end": "2024-09-09"}, partition={}
                ),
            ]

            records = [
                {"id": "444", "name": "Yongen-jaya", "updated_at": "2024-08-10"},
                {"id": "scramble", "name": "Shibuya", "updated_at": "2024-08-10"},
                {"id": "aoyama", "name": "Aoyama-itchome", "updated_at": "2024-08-10"},
                {"id": "shin123", "name": "Shinjuku", "updated_at": "2024-08-10"},
            ]
        case "party_members":
            slices = [
                # Slices used during first incremental sync
                StreamSlice(
                    cursor_slice={"start": "2024-07-01", "end": "2024-07-15"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-07-16", "end": "2024-07-30"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-07-31", "end": "2024-08-14"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-15", "end": "2024-08-29"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-30", "end": "2024-09-09"}, partition={}
                ),
                # Slices used during incremental checkpoint sync. Unsuccessful partitions use the P5D lookback window which explains
                # the skew of records midway through
                StreamSlice(
                    cursor_slice={"start": "2024-07-01", "end": "2024-07-16"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-07-30", "end": "2024-08-13"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-08-14", "end": "2024-08-14"}, partition={}
                ),
                StreamSlice(
                    cursor_slice={"start": "2024-09-04", "end": "2024-09-09"}, partition={}
                ),
            ]

            records = [
                {
                    "id": "amamiya",
                    "first_name": "ren",
                    "last_name": "amamiya",
                    "updated_at": "2024-07-10",
                },
                {
                    "id": "nijima",
                    "first_name": "makoto",
                    "last_name": "nijima",
                    "updated_at": "2024-08-10",
                },
                {
                    "id": "yoshizawa",
                    "first_name": "sumire",
                    "last_name": "yoshizawa",
                    "updated_at": "2024-09-10",
                },
            ]
        case "palaces":
            slices = [StreamSlice(cursor_slice={}, partition={})]

            records = [
                {"id": "0", "world": "castle", "owner": "kamoshida"},
                {"id": "1", "world": "museum", "owner": "madarame"},
                {"id": "2", "world": "bank", "owner": "kaneshiro"},
                {"id": "3", "world": "pyramid", "owner": "futaba"},
                {"id": "4", "world": "spaceport", "owner": "okumura"},
                {"id": "5", "world": "casino", "owner": "nijima"},
                {"id": "6", "world": "cruiser", "owner": "shido"},
            ]

        case "party_members_skills":
            slices = [StreamSlice(cursor_slice={}, partition={})]

            records = [
                {"id": "0", "name": "hassou tobi"},
                {"id": "1", "name": "mafreidyne"},
                {"id": "2", "name": "myriad truths"},
            ]
        case _:
            raise ValueError(f"Stream '{stream_name}' does not have associated mocked records")

    return {
        (_slice.get("start"), _slice.get("end")): [
            Record(data=stream_data, associated_slice=_slice) for stream_data in records
        ]
        for _slice in slices
    }


def get_records_for_stream(
    stream_name: str, messages: List[AirbyteMessage]
) -> List[AirbyteRecordMessage]:
    return [
        message.record
        for message in messages
        if message.record and message.record.stream == stream_name
    ]


def get_states_for_stream(
    stream_name: str, messages: List[AirbyteMessage]
) -> List[AirbyteStateMessage]:
    return [
        message.state
        for message in messages
        if message.state and message.state.stream.stream_descriptor.name == stream_name
    ]
