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
    tags={"business_relevance": "personalization"}
)
