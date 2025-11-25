
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

from ai.chronon.query import (
    Query,
    select,
)
from ai.chronon.utils import get_staging_query_output_table_name
from ai.chronon.api import ttypes
from ai.chronon.staging_query import StagingQueryEventSource

from staging_queries.sample_team import sample_staging_query
from staging_queries.sample_team.sample_staging_query_view import v1 as staging_query_view


def basic_event_source(table):
    return ttypes.Source(events=ttypes.EventSource(
        table=table,
        query=Query(
            selects=select(
                event="event_expr",
                group_by_subject="group_by_expr",
            ),
            start_partition="2021-04-09",
            time_column="ts",
        ),
    ))


# Sample Event Source used in tests.
event_source = ttypes.Source(events=ttypes.EventSource(
    table="sample_namespace.sample_table_group_by",
    query=Query(
        selects=select(
            event="event_expr",
            group_by_subject="group_by_expr",
        ),
        start_partition="2021-04-09",
        time_column="ts",
    ),
))

# Sample Event Source with topic for real-time/JoinSource streaming
real_time_event_source = ttypes.Source(events=ttypes.EventSource(
    table="sample_namespace.sample_table_group_by",
    topic="sample_topic",
    query=Query(
        selects=select(
            event="event_expr",
            group_by_subject="group_by_expr",
        ),
        start_partition="2021-04-09",
        time_column="ts",
    ),
))

# Sample Entity Source
entity_source = ttypes.Source(entities=ttypes.EntitySource(
    snapshotTable="sample_table.sample_entity_snapshot",
    # hr partition is not necessary - just to demo that we support various 
    # partitioning schemes
    mutationTable="sample_table.sample_entity_mutations/hr=00:00",
    mutationTopic="sample_topic",
    query=Query(
        start_partition='2021-03-01',
        selects=select(
            group_by_subject='group_by_subject_expr',
            entity='entity_expr',
        ),
        time_column="ts"
    ),
))

batch_entity_source = ttypes.Source(entities=ttypes.EntitySource(
    snapshotTable="sample_table.sample_entity_snapshot",
    query=Query(
        start_partition='2021-03-01',
        selects=select(
            group_by_subject='group_by_subject_expr',
            entity='entity_expr',
        ),
        time_column="ts"
    ),
))

# Sample Entity Source derived from a staging query.
staging_entities=ttypes.Source(entities=ttypes.EntitySource(
    snapshotTable="sample_namespace.{}".format(get_staging_query_output_table_name(sample_staging_query.v1)),
    query=Query(
        start_partition='2021-03-01',
        selects=select(**{
            'impressed_unique_count_1d': 'impressed_unique_count_1d',
            'viewed_unique_count_1d': 'viewed_unique_count_1d',
            's2CellId': 's2CellId',
            'place_id': 'place_id'
        })
    )
))


# A Source that was deprecated but still relevant (requires stitching).
events_until_20210409 = ttypes.Source(events=ttypes.EventSource(
    table="sample_namespace.sample_table_group_by",
    query=Query(
        start_partition='2021-03-01',
        end_partition='2021-04-09',
        selects=select(**{
            'group_by_subject': 'group_by_subject_expr_old_version',
            'event': 'event_expr_old_version',
        }),
        time_column="UNIX_TIMESTAMP(ts) * 1000"
    ),
))

# The new source
events_after_20210409 = ttypes.Source(events=ttypes.EventSource(
    table="sample_namespace.another_sample_table_group_by",
    query=Query(
        start_partition='2021-03-01',
        selects=select(**{
            'group_by_subject': 'possibly_different_group_by_subject_expr',
            'event': 'possibly_different_event_expr',
        }),
        time_column="__timestamp"
    ),
))

# Sample Staging Query Event Source  
staging_query_event_source = StagingQueryEventSource(
    staging_query=staging_query_view,
    query=Query(
        selects=select("user_id", "session_length", "page_views"),
        time_column="ts"  # Assuming there's a ts column in the view
    ),
    is_cumulative=False
)
