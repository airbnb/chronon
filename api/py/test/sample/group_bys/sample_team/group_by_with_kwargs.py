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
    sources=[
        test_sources.events_until_20210409,
        test_sources.events_after_20210409,
    ],
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.SUM),
        Aggregation(input_column="event", operation=Operation.SUM, windows=[Window(7, TimeUnit.DAYS)]),
    ],
    additional_argument="To be placed in customJson",
)
