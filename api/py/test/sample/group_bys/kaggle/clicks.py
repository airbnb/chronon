from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
    Accuracy
)

"""
This GroupBy aggregates clicks by the ad_id primary key

It's setup to resemble a streaming GroupBy, however since we're working with static CSV data, we don't have an actual
streaming source of data to work with, so the `topic` field is unset (that doesn't hinder creating training data, which reads from the offline table).
"""


source = Source(
    events=EventSource(
        isCumulative=True,
        table="kaggle_outbrain.clicks_train",
        topic="some_topic", # You would set your streaming source topic here if you had one
        query=Query(
            selects=select("ad_id", "clicked"),
            time_column="ts")
    ))

ad_streaming = GroupBy(
    sources=[source],
    keys=["ad_id"], # We use the ad_id column as our primary key
    aggregations=[Aggregation(
            input_column="clicked",
            operation=Operation.SUM,
            windows=[Window(length=3, timeUnit=TimeUnit.DAYS)]
        ),
        Aggregation(
            input_column="clicked",
            operation=Operation.COUNT,
            windows=[Window(length=3, timeUnit=TimeUnit.DAYS)]
        ),
        Aggregation(
            input_column="clicked",
            operation=Operation.AVERAGE,
            windows=[Window(length=3, timeUnit=TimeUnit.DAYS)]
        )
    ],
    accuracy=Accuracy.TEMPORAL # Here we use temporal accuracy so that training data backfills mimic streaming updates
)
