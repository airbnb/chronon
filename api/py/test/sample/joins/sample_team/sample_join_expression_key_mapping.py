"""
Sample Join demonstrating expression-based key_mapping.

The left side has entity_id strings like "Video:123", and the right side
GroupBy is keyed on video_id (a numeric type). The expression in keyMapping
extracts and casts the numeric ID from the entity string:

    key_mapping={"entity_id": "CAST(SPLIT(entity_id, ':')[1] AS BIGINT) AS video_id"}
"""
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, select
from ai.chronon.api import ttypes
from group_bys.sample_team import event_sample_group_by

left_part = ttypes.Source(
    events=ttypes.EventSource(
        table="sample_namespace.events_with_entity_id",
        query=Query(
            selects=select(
                entity_id="entity_id",
            ),
            start_partition="2021-04-09",
            time_column="ts",
        ),
    )
)

v1 = Join(
    left=left_part,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={
                "entity_id": "CAST(SPLIT(entity_id, ':')[1] AS BIGINT) AS group_by_subject",
            },
            prefix="video",
        ),
    ],
)