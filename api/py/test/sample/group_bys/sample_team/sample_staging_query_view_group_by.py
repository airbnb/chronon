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
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit
from ai.chronon.staging_query import get_staging_query_dependencies
from staging_queries.sample_team.sample_staging_query_view import v1 as staging_query

"""
Sample GroupBy that uses a staging query view as its source.
"""

# Use source from test_sources
source = test_sources.staging_query_event_source

# Get explicit dependencies for the staging query view
staging_query_deps = get_staging_query_dependencies(staging_query)

# Define window sizes
window_sizes = [Window(length=day, timeUnit=TimeUnit.DAYS) for day in [7, 14, 30]]

# Create the GroupBy with explicit staging query dependencies
v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    dependencies=staging_query_deps,  # Explicit signal partition dependencies
    aggregations=[
        Aggregation(
            input_column="session_length",
            operation=Operation.SUM,
            windows=window_sizes
        ),
        Aggregation(
            input_column="session_length", 
            operation=Operation.AVERAGE,
            windows=window_sizes
        ),
        Aggregation(
            input_column="page_views",
            operation=Operation.COUNT,
            windows=window_sizes  
        ),
        Aggregation(
            input_column="page_views",
            operation=Operation.MAX,
        ),
    ],
)