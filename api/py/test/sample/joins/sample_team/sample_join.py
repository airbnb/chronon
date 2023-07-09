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
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
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

never = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    offline_schedule='@never',
)

consistency_check = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    check_consistency=True,
)

no_log_flattener = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    sample_percent=0.0,
)
