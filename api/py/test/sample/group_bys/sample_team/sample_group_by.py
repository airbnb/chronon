from sources import test_sources
from ai.zipline.group_by import (
    GroupBy,
    Aggregation,
    Operation,
)


v1 = GroupBy(
    sources=test_sources.staging_entities,
    keys=["s2CellId", "place_id"],
    aggregations=[
        Aggregation(input_column="impressed_unique_count_1d", operation=Operation.SUM),
        Aggregation(input_column="viewed_unique_count_1d", operation=Operation.SUM),
    ],
    production=False,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    output_namespace="sample_namespace",
)
