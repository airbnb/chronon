"""
Sample Label Join
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

from ai.chronon.join import Join, JoinPart, LabelPart
from ai.chronon.group_by import (
    GroupBy,
)

label_part_group_by = GroupBy(
    name="sample_label_group_by",
    sources=test_sources.batch_entity_source,
    keys=["group_by_subject"],
    aggregations=None,
    online=False,
)

v1 = Join(
    left=test_sources.event_source,
    output_namespace="sample_namespace",
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=group_by_with_kwargs.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
    ],
    label_part=LabelPart([
            JoinPart(
                group_by=label_part_group_by
            ),
        ],
        left_start_offset=30,
        left_end_offset=10,
        label_offline_schedule="@weekly"
        ),
    additional_args={
        'custom_arg': 'custom_value'
    },
    additional_env={
        'custom_env': 'custom_env_value'
    },
    online=False
)
