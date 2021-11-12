from ai.zipline.api import ttypes as api
from ai.zipline.group_by import GroupBy

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
