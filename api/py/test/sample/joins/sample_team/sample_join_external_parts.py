"""
Sample Join
"""
from group_bys.sample_team import sample_group_by
from sources import test_sources

from ai.chronon.join import (
    Join,
    JoinPart,
    ExternalPart,
    ExternalSource,
    DataType,
    ContextualSource
)

v1 = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1)],
    online_external_parts=[
        ExternalPart(
            ExternalSource(
                name="test_external_source",
                team="chronon",
                key_fields=[
                    ("key", DataType.LONG)
                ],
                value_fields=[
                    ("value_str", DataType.STRING),
                    ("value_long", DataType.LONG),
                    ("value_bool", DataType.BOOLEAN)
                ]
            )
        ),
        ExternalPart(
            ContextualSource(
                fields=[
                    ("context_str", DataType.STRING),
                    ("context_long", DataType.LONG),
                ],
                team="chronon"
            )
        )
    ],
    table_properties={
        "config_json": """{"sample_key": "sample_value"}"""
    },
    output_namespace="sample_namespace"
)
