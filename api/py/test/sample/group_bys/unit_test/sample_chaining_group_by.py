"""
Sample Chaining Group By with join as a source
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

from ai.chronon.api import ttypes
from ai.chronon.group_by import Aggregation, GroupBy, Operation
from ai.chronon.query import Query, select
from joins.unit_test import sample_parent_join

chaining_group_by_v1 = GroupBy(
    sources=ttypes.Source(
        joinSource=ttypes.JoinSource(
            join=sample_parent_join.parent_join,
            query=Query(
                selects=select(
                    event="event_expr",
                    group_by_subject="group_by_expr",
                ),
                start_partition="2023-04-15",
                time_column="ts",
            ),
        )
    ),
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.LAST),
    ],
    online=True,
    production=True,
)
