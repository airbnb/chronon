# from ai.zipline.shorthand import *
from ai.zipline.api import ttypes as api
from ai.zipline.group_by import GroupBy

"""
v1 = GroupBy(
    name="event_source_from_shorthand",
    table="sample_namespace.sample_table_group_by",
    model=DataModel.EVENTS,
    keys=[("group_by_subject", "group_by_expr")],
    aggs=[
        Agg(
            column="event",
            expression="event_expr",
            op=SUM,
            window="7d",
        ),
        Agg(
            column="event",
            expression="event_expr",
            op=SUM,
        ),
    ],
    start_partition="2021-04-09",
    output_namespace="sample_namespace",
    online=False,
    production=False,
)
"""

v1 = GroupBy(
    sources=[api.Source(
        events=api.EventSource(
            query=api.Query(
                selects={
                    "event": "event_expr",
                    "group_by_subject": "group_by_subject",
                    "group_by_expr": "group_by_expr"
                },
                startPartition="2021-04-09"
            )
        )
    )],
    keys=["group_by_subject", "group_by_expr"],
    aggregations=[
        api.Aggregation(
            inputColumn="event",
            operation=api.Operation.SUM,
            windows=[api.Window(length=7, timeUnit=api.TimeUnit.DAYS)]
        ),
        api.Aggregation(
            inputColumn="event",
            operation=api.Operation.SUM
        )
    ],
    online=False,
    production=False,
    output_namespace="sample_namespace"
)
