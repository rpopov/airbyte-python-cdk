#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Iterable, List, Mapping, MutableMapping, Union

from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor

from airbyte_cdk.sources.declarative.extractors.record_extractor import add_service_key

# The name of the service key that holds a reference to the original root response
SERVICE_KEY_ROOT = "root"

# The name of the service key that holds a reference to the owner object
SERVICE_KEY_PARENT = "parent"

@dataclass
class DpathEnhancingExtractor(DpathExtractor):
    """
    Like the DpathExtractor, extract records from a response by following a path of names  of nested objects,
    while adding specific service fields to the extracted records to facilitate the further processing.
    """

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        """
        See  DpathExtractor
        """
        super().__post_init__(parameters)

    def update_body(self, body: Any) -> Any:
        """
        In each nested object in the body add a servive key "parent" to refer to the owner object.
        For the root object/body the owner is None.
        Example:
        body = { "a":1, "b":2, "c":{"d":4}}
        result = { "a":1,
                   "b":2,
                   "c":{"d":4,
                        parent: { "a":1,
                                  "b":2,
                                  "c":{"d":4, "parent":....},
                                  "parent":none
                                 }},
                   "parent":none}

        Example:
        body = { "a":1, "b":2, "c":[{"d":4},{"e",5}]}
        result = { "a":1,
                   "b":2,
                   "c":[{"d":4, "parent":{ "a":1, "b":2, "c":[{"d":4},{"e",5}]}},
                        {"e",5, "parent":{ "a":1, "b":2, "c":[{"d":4},{"e",5}]}}
                       ],
                   "parent": none
                 }

        :param body: the original response body. Not to be changed
        :return: a copy of the body enhanced in a subclass-specific way. None only when body is None.
        """
        return self._set_parent(body,None)

    def _set_parent(self,body: Any, parent:Any) -> Any:
        """
        :param body: the original response body. Not to be changed
        :param parent: the paarent object that owns/has as nesteed the body object
        :return: a copy of the body enhanced in a subclass-specific way. None only when body is None.
        """
        if isinstance(body, dict):
            result = add_service_key(dict(), SERVICE_KEY_PARENT, parent)
            for k,v in body.items():
                result[k] = self._set_parent(v,result)
        elif isinstance(body, list):
            result = [self._set_parent(v,parent) for v in body]
        else:
            result = body
        return result

    def update_record(self, record: Any, root: Any) -> Any:
        """
        Change the extracted record in a subclass-specific way. Override in subclasses.
        :param record: the original extracted record. Not to be changed. Not None.
        :param root: the original body the record is extracted from.
        :return: a copy of the record changed or enanced in a subclass-specific way.
        """
        return add_service_key(record, SERVICE_KEY_ROOT, root)
