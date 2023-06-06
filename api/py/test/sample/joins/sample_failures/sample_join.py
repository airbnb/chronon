"""
Sample Join
"""
from sources import test_sources
from group_bys.sample_team import sample_group_by
from ai.chronon.join import (
    Join,
    JoinPart,
)


v1 = Join(
    left=test_sources.staging_entities,
    right_parts=[
        JoinPart(group_by=sample_group_by.v1, tags={"experimental": True}),
        JoinPart(group_by=sample_group_by.v1)],
    table_properties={
        "config_json": """{"sample_key": "sample_value"}"""
    },
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    env={
        "backfill": {
            "EXECUTOR_MEMORY": "9G"
        },
    },
)
