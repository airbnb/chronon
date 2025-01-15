"""
Sample Join
"""

#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from sources import test_sources
from group_bys.sample_team import (
    event_sample_group_by,
    entity_sample_group_by_from_module,
    group_by_with_kwargs,
)

from ai.chronon.join import (
    Join,
    JoinPart,
    ExternalPart,
    ExternalSource,
    DataType,
    ContextualSource,
    Derivation
)


v1 = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        )
    ],
    online_external_parts=[
        ExternalPart(
            ExternalSource(
                name="test_external_source",
                team="chronon",
                key_fields=[
                    ("key", DataType.LONG)
                ],
                value_fields=[
                    ("value_str", DataType.STRING),
                    ("value_long", DataType.LONG),
                    ("value_bool", DataType.BOOLEAN)
                ]
            )
        ),
        ExternalPart(
            ContextualSource(
                fields=[
                    ("context_str", DataType.STRING),
                    ("context_long", DataType.LONG),
                ],
                team="chronon"
            )
        )
    ],
    derivations=[
        *[
            Derivation(name=c, expression=f"ext_test_external_source_{c}") for c in [
                "value_str", "value_long", "value_bool"
            ]
        ],
        *[
            Derivation(name=c, expression=f"ext_contextual_{c}") for c in [
                "context_str", "context_long"
            ]
        ],
        Derivation(
            name="*",
            expression="*"
        )
    ]
)
