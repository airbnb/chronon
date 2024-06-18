"""
Sample Chaining Join
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

from ai.chronon.join import Join, JoinPart
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Accuracy,
    Operation,
)
from ai.chronon.api import ttypes
from ai.chronon.query import (
    Query,
    select,
)

parent_join = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
    ],
    online=True,
    check_consistency=True,
    historical_backfill=False,
)

chaining_group_by_v1 = GroupBy(
    name="sample_team.sample_chaining_group_by",
    sources=ttypes.Source(joinSource=ttypes.JoinSource(
        join=parent_join,
        query=Query(
            selects=select(
                event="event_expr",
                group_by_subject="group_by_expr",
            ),
            start_partition="2023-04-15",
            time_column="ts",
        ))),
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.LAST),
    ],
    accuracy=Accuracy.TEMPORAL,
    online=True,
    production=True,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    output_namespace="sample_namespace",
)

v1 = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=chaining_group_by_v1,
            key_mapping={'subject': 'user_id'},
        ),
    ],
    additional_args={
        'custom_arg': 'custom_value'
    },
    additional_env={
        'custom_env': 'custom_env_value'
    },
    online=True,
    check_consistency=True
)
