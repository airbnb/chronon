from ai.zipline.shorthand import *


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
    outputNamespace="sample_namespace",
    online=False,
    production=False,
)
