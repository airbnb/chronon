"""
Sample group by
"""
from ai.zipline.api import ttypes as api
from ai.zipline.group_by import group_by


v1 = group_by(
    sources=[
        api.Source(
            events=api.EventSource(
                table="sample_namespace.sample_table_group_by",
                query=api.Query(
                    startPartition='2021-03-01',
                    selects={
                        'group_by_subject': 'group_by_subject_expr',
                        'event': 'event_expr',
                    },
                    timeColumn="UNIX_TIMESTAMP(ts) * 1000"
                ),
            ),
        ),
        api.Source(
            events=api.EventSource(
                table="sample_namespace.another_sample_table_group_by",
                query=api.Query(
                    startPartition='2021-03-01',
                    selects={
                        'group_by_subject': 'possibly_different_group_by_subject_expr',
                        'event': 'possibly_different_event_expr',
                    },
                    timeColumn="UNIX_TIMESTAMP(ts) * 1000"
                ),
            ),
        )
    ],
    keys=["group_by_subject"],
    aggregations=[
        api.Aggregation(
            inputColumn="event",
            operation=api.Operation.SUM
        ),
        api.Aggregation(
            inputColumn="event",
            operation=api.Operation.SUM,
            windows=[api.Window(length=7, timeUnit=api.TimeUnit.DAYS)]
        ),
    ],
)
