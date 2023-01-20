from ai.chronon.group_by import (
    Accuracy,
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
)
from ai.chronon.api.ttypes import EventSource
from ai.chronon.query import Query, select

# TODO Add data tests
transactions_source = EventSource(
        table="namespace.transaction_table",  # TODO fill this
        topic="transactions_kafka_topic",  # TODO fill this - only necessary for online
        query=Query(selects=select("amount")),
)

v1 = GroupBy(
    sources=[transactions_source],
    keys=["merchant_id"],
    aggregations=[
        Aggregation(
            input_column="amount",
            operation=Operation.AVERAGE,
            windows=[Window(7, timeUnit=TimeUnit.DAYS)],
            buckets=["card_id"]
        ),
        Aggregation(
            input_column="amount",
            operation=Operation.VARIANCE,
            windows=[Window(7, timeUnit=TimeUnit.DAYS)],
            buckets=["card_id"]
        )
    ],
    accuracy=Accuracy.TEMPORAL
)
