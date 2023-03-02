from sources import test_sources
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    TimeUnit,
    Window,
)


v1 = GroupBy(
    sources=test_sources.event_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(
            input_column="event",
            operation=Operation.SUM,
            windows=[Window(length=7, timeUnit=TimeUnit.DAYS)],
            additional_metadata={"DETAILED_TYPE": "CONTINUOUS"}
        ),
        Aggregation(
            input_column="event",
            operation=Operation.SUM
        ),
        Aggregation(
            input_column="event",
            operation=Operation.APPROX_PERCENTILE([0.99, 0.95, 0.5], k=200), # p99, p95, Median
        )
    ],
    online=True,
    output_namespace="sample_namespace",
    additional_metadata={"TO_DEPRECATE": True}
)
