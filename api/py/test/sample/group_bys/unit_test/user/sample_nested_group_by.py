from ai.chronon.api.ttypes import Aggregation, Operation, TimeUnit, Window
from ai.chronon.group_by import Aggregations, GroupBy
from ai.chronon.query import Query, select
from ai.chronon.source import EventSource

source = EventSource(
    table="random_table_name",
    query=Query(
        selects=select(
            user="id_item",
            play="if(transaction_type='A', 1, 0)",
            pause="if(transaction_type='B', 1, 0)",
        ),
        start_partition="2023-03-01",
        time_column="UNIX_TIMESTAMP(ts) * 1000",
    ),
)

windows = [
    Window(length=30, timeUnit=TimeUnit.DAYS),
]

v1 = GroupBy(
    keys=["user"],
    sources=source,
    aggregations=Aggregations(
        play_count=Aggregation(operation=Operation.SUM, inputColumn="play", windows=windows),
        pause_count=Aggregation(operation=Operation.SUM, inputColumn="pause", windows=windows),
    ),
    online=True,
)
