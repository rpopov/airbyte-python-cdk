#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import pytest
from typing import Mapping
from airbyte_cdk.sources.declarative.extractors.record_extractor import remove_service_keys, verify_service_keys_exist, SERVICE_KEY_PREFIX


@pytest.mark.parametrize(
    "original, expected",
    [
        ({},{}),
        ({"k":"v"},{"k":"v"}),
        ({"k": "v", "k2": "v"}, {"k": "v", "k2": "v"}),
        ({SERVICE_KEY_PREFIX+"k": "v"}, {}),
        ({SERVICE_KEY_PREFIX+"k": "v","k": "v"}, {"k": "v"}),
        ({SERVICE_KEY_PREFIX+"k": "v","k": "v", "k2": "v"}, {"k": "v", "k2": "v"})
    ]
)
def test_remove_service_keys(original: Mapping, expected: Mapping):
    assert remove_service_keys(original) == expected

@pytest.mark.parametrize(
    "original",
    [
        ({SERVICE_KEY_PREFIX+"k": "v"}),
        ({SERVICE_KEY_PREFIX+"k": "v","k": "v"}),
        ({SERVICE_KEY_PREFIX+"k": "v","k": "v", "k2": "v"})
    ]
)
def test_verify_service_keys(original: Mapping):
    verify_service_keys_exist(original)

@pytest.mark.parametrize(
    "original",
    [
        ({}),
        ({"k":"v"}),
        ({"k": "v", "k2": "v"})
    ]
)
def test_verify_no_service_keys(original: Mapping):
    try:
        verify_service_keys_exist(original)
        success = False
    except: # OK, expected
        success = True

    assert success, "Expected no service keys were found"
