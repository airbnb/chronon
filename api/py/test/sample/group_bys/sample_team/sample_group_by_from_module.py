"""
Sample group by
"""
from ai.zipline.api import ttypes as api
from ai.zipline.group_by import GroupBy


v1 = GroupBy(
    sources=api.Source(
        events=api.EventSource(
            table="sample_namespace.sample_table",
            query=api.Query(
                startPartition='2021-03-01',
                selects={
                    'subject': 'subject_expr',
                    'event': 'event_expr',
                },
                timeColumn="UNIX_TIMESTAMP(ts) * 1000"
            ),
        ),
    ),
    keys=["subject"],
    aggregations=[
        api.Aggregation(
            inputColumn="event",
            operation=api.Operation.SUM
        ),
    ],
)
