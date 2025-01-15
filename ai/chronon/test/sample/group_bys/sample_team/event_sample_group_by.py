
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
    TimeUnit,
    Window,
)


v1 = GroupBy(
    sources=test_sources.event_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(
            input_column="event",
            operation=Operation.SUM,
            windows=[Window(length=7, timeUnit=TimeUnit.DAYS)],
            tags={"DETAILED_TYPE": "CONTINUOUS"}
        ),
        Aggregation(
            input_column="event",
            operation=Operation.SUM
        ),
        Aggregation(
            input_column="event",
            operation=Operation.APPROX_PERCENTILE([0.99, 0.95, 0.5], k=200), # p99, p95, Median
        )
    ],
    online=True,
    output_namespace="sample_namespace",
    tags={"TO_DEPRECATE": True}
)
