"""
Sample group by
"""
from sources import test_sources
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
)


v1 = GroupBy(
    sources=test_sources.entity_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(input_column="entity", operation=Operation.LAST),
        Aggregation(input_column="entity", operation=Operation.LAST, windows=[Window(7, TimeUnit.DAYS)]),
    ],
    online=True,
)
