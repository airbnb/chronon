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
ip_successes_source = EventSource(
        table="namespace.ip_successes",  # TODO fill this
        topic="ip_successes_kafka_topic",  # TODO fill this - only necessary for online
        query=Query(selects=select("ip", "success")),
)

# use bucketing to produce map of {ip: success_rate} and the client processes into avg
success_rate_bucketed = GroupBy(
    sources=[ip_successes_source],
    keys=["merchant_id"],
    aggregations=[
        Aggregation(
            input_column="success",
            operation=Operation.AVERAGE,
            windows=[Window(7, timeUnit=TimeUnit.DAYS)],
            buckets=["ip"]
        )
    ],
    accuracy=Accuracy.TEMPORAL
)
