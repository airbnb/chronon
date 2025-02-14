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

from ai.chronon.group_by import Aggregation, GroupBy, Operation
from ai.chronon.join import Join, JoinPart
from sources import test_sources

inline_group_by = GroupBy(
    name="unit_test.user_inline_group_by",
    sources=test_sources.event_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.SUM),
    ],
    online=True,
)

v1 = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=inline_group_by,
            key_mapping={"subject": "group_by_subject"},
        ),
    ],
)
