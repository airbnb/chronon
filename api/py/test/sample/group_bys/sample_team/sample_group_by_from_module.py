"""
Sample group by
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
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
)


v1 = GroupBy(
    sources=[
        test_sources.events_until_20210409,
        test_sources.events_after_20210409,
    ],
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.SUM),
        Aggregation(input_column="event", operation=Operation.SUM, windows=[Window(7, TimeUnit.DAYS)]),
    ],
)
