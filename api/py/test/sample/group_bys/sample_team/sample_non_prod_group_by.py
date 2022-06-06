from sources import test_sources
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
)


v1 = GroupBy(
    sources=test_sources.event_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.SUM, windows=[Window(7, TimeUnit.DAYS)]),
        Aggregation(input_column="event", operation=Operation.SUM)
    ],
    online=False,
    production=False,
    output_namespace="sample_namespace"
)
