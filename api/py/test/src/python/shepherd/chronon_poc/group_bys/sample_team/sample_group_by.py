from src.python.shepherd.chronon_poc.sources import events
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
)


v1 = GroupBy(
    sources=events.merchant_card_right_part,
    keys=["merchant", "card"],
    aggregations=[
        Aggregation(input_column="amount", operation=Operation.SUM),
    ],
    production=False,
    table_properties={},
    output_namespace="sample_namespace",
    online=True,
)