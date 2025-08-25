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

from ai.chronon.api.ttypes import StagingQuery, MetaData, Query

"""
Sample staging query for testing view functionality.
This staging query creates a view instead of a regular table.
"""

# Create a staging query that outputs as a view
v1 = StagingQuery(
    metaData=MetaData(
        name="sample_staging_query_view",
        outputNamespace="sample_team",
    ),
    query=Query(
        # Note: No date templates ({{ start_date }}, {{ end_date }}) for view mode
        selects={
            "user_id": "user_id", 
            "session_length": "session_length_seconds",
            "page_views": "page_view_count"
        },
        startPartition="2023-01-01",
        endPartition="2023-12-31",
    ),
    createView=True  # This enables view mode with signal partition tracking
)