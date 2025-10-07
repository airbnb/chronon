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
    TimeUnit
)

"""
This GroupBy aggregates metrics about user purchases and repayment data.
Replaces the Airflow DAG for materializing Paybright repayment data.
"""

# Source for purchase/repayment events
source = Source(
    events=EventSource(
        table="data.purchases",  # Purchase/repayment events table
        topic=None,  # Could be configured for real-time streaming
        query=Query(
            selects=select(
                "user_id",
                "purchase_amount", 
                "repayment_amount",
                "transaction_type",
                "merchant_id",
                "country_code"
            ),
            time_column="ts"  # Event timestamp
        )
    )
)

# Define window sizes for aggregations
window_sizes = [
    Window(length=day, timeUnit=TimeUnit.DAYS) 
    for day in [1, 7, 14, 30, 90]
]

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],  # Aggregate by user
    online=True,  # Enable online serving
    aggregations=[
        # Purchase amount aggregations
        Aggregation(
            input_column="purchase_amount",
            operation=Operation.SUM,
            windows=window_sizes
        ),
        Aggregation(
            input_column="purchase_amount",
            operation=Operation.COUNT,
            windows=window_sizes
        ),
        Aggregation(
            input_column="purchase_amount",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ),
        Aggregation(
            input_column="purchase_amount",
            operation=Operation.MAX,
            windows=window_sizes
        ),
        
        # Repayment amount aggregations
        Aggregation(
            input_column="repayment_amount",
            operation=Operation.SUM,
            windows=window_sizes
        ),
        Aggregation(
            input_column="repayment_amount",
            operation=Operation.COUNT,
            windows=window_sizes
        ),
        Aggregation(
            input_column="repayment_amount",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ),
        
        # Transaction type counts
        Aggregation(
            input_column="transaction_type",
            operation=Operation.COUNT,
            windows=window_sizes
        ),
        
        # Merchant diversity
        Aggregation(
            input_column="merchant_id",
            operation=Operation.COUNT_DISTINCT,
            windows=window_sizes
        ),
        
        # Recent transaction history
        Aggregation(
            input_column="purchase_amount",
            operation=Operation.LAST_K(5),
        ),
        Aggregation(
            input_column="repayment_amount", 
            operation=Operation.LAST_K(3),
        ),
    ],
)
