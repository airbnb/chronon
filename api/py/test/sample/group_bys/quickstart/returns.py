
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

from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
    Accuracy,
)

"""
This GroupBy aggregates metrics about a user's previous purchases in various windows.
"""

source = Source(
    events=EventSource(
        table="data.returns", # This points to the log table with historical return events
        topic="events.returns/fields=ts,return_id,user_id,product_id,refund_amt/host=kafka/port=9092",
        query=Query(
            selects=select("user_id","refund_amt"), # Select the fields we care about
            time_column="ts") # The event time
    ))

window_sizes = [Window(length=day, timeUnit=TimeUnit.DAYS) for day in [3, 14, 30]] # Define some window sizes to use below

v1 = GroupBy(
    sources=[source],
    keys=["user_id"], # We are aggregating by user
    online=True,
    aggregations=[Aggregation(
            input_column="refund_amt",
            operation=Operation.SUM,
            windows=window_sizes
        ), # The sum of purchases prices in various windows
        Aggregation(
            input_column="refund_amt",
            operation=Operation.COUNT,
            windows=window_sizes
        ), # The count of purchases in various windows
        Aggregation(
            input_column="refund_amt",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ),
        Aggregation(
            input_column="refund_amt",
            operation=Operation.LAST_K(2),
        ),
    ],
)
