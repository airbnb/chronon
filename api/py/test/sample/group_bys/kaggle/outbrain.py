
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


from ai.chronon.utils import get_staging_query_output_table_name
from staging_queries.kaggle.outbrain import base_table


def outbrain_left_events(*cols):
    return Source(events=EventSource(
        isCumulative=True,
        table=get_staging_query_output_table_name(base_table),
        query=Query(
            selects=select(*cols),
            time_column="ts",
        ),
    ))


def ctr_group_by(*keys):
    return GroupBy(
        sources=[outbrain_left_events(*(list(keys) + ["clicked"]))],
        keys=list(keys),
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
        accuracy=Accuracy.TEMPORAL
    )


ad_doc = ctr_group_by("ad_id", "document_id")
ad = ctr_group_by("ad_id")
ad_uuid = ctr_group_by("ad_id", "uuid")
ad_platform = ctr_group_by("ad_id", "platform", "geo_location")
