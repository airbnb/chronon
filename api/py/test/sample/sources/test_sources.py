from ai.chronon.query import (
    Query,
    select,
)
from ai.chronon.utils import get_staging_query_output_table_name
from ai.chronon.api import ttypes

from staging_queries.sample_team import sample_staging_query

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
