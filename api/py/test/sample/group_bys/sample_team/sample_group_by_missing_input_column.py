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
        Aggregation(operation=Operation.COUNT),
        Aggregation(operation=Operation.COUNT),
    ],
    production=False,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    online=True,
    output_namespace="sample_namespace",
)
