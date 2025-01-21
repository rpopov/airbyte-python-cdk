#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from typing import Mapping

import pytest

from airbyte_cdk.sources.declarative.extractors.record_extractor import (
    _SERVICE_KEY_PREFIX,
    assert_service_keys_exist,
    exclude_service_keys,
    is_service_key,
)


@pytest.mark.parametrize(
    "original, expected",
    [
        ({}, {}),
        ({"k": "v"}, {"k": "v"}),
        ({"k": "v", "k2": "v"}, {"k": "v", "k2": "v"}),
        ({_SERVICE_KEY_PREFIX + "k": "v"}, {}),
        ({_SERVICE_KEY_PREFIX + "k": "v", "k": "v"}, {"k": "v"}),
        ({_SERVICE_KEY_PREFIX + "k": "v", "k": "v", "k2": "v"}, {"k": "v", "k2": "v"}),
    ],
)
def test_exclude_service_keys(original: Mapping, expected: Mapping):
    assert exclude_service_keys(original) == expected


@pytest.mark.parametrize(
    "original",
    [
        ({_SERVICE_KEY_PREFIX + "k": "v"}),
        ({_SERVICE_KEY_PREFIX + "k": "v", "k": "v"}),
        ({_SERVICE_KEY_PREFIX + "k": "v", "k": "v", "k2": "v"}),
    ],
)
def test_verify_service_keys(original: Mapping):
    assert_service_keys_exist(original)


@pytest.mark.parametrize("original", [({}), ({"k": "v"}), ({"k": "v", "k2": "v"})])
def test_verify_no_service_keys(original: Mapping):
    try:
        assert_service_keys_exist(original)
        success = False
    except:  # OK, expected
        success = True

    assert success, "Expected no service keys were found"


def test_service_field():
    assert is_service_key(_SERVICE_KEY_PREFIX + "name")


def test_regular_field():
    assert not is_service_key("name")
