"""
Temporal entity sample group by
"""
from sources import test_sources
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Accuracy,
)


v0 = GroupBy(
    sources=test_sources.entity_source,
    keys=["group_by_subject"],
    aggregations=[Aggregation(input_column="entity", operation=Operation.LAST)],
    accuracy=Accuracy.TEMPORAL,
)
