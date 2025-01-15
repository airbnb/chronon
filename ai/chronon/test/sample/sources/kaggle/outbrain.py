

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
from ai.chronon.utils import get_staging_query_output_table_name
from staging_queries.kaggle.outbrain import base_table

"""
Sources allow one-to-one transformations (i.e. row level transformations like ROUND, IF, etc.), but no joins (for this you must use a StagingQuery) or Aggregation (these occur in GroupBy).

Sources are used as components in GroupBys (which can define aggregations on top of a source for a given primary key), or as the left side of a Join.
"""

def outbrain_left_events(*columns):
    """
    Defines a source based off of the output table of the `base_table` StagingQuery.
    """
    return Source(events=EventSource(
        table=get_staging_query_output_table_name(base_table),
        query=Query(
            selects=select(*columns),
            time_column="ts",
        ),
    ))
