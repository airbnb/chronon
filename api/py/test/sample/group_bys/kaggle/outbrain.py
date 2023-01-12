
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

"""
This file defines a number of GroupBys in a more programatic way, leveraging helper functions that act
as templates. The result is the same as creating multiple files that look more like individual "configuration" (as
we did in the clicks_by_ad GroupBy), but this can be more concise and easily managed.

There is no definitive guidance on when to use one approach over the other. In this case we have multiple
GroupBys that share a source and aggregations, and only vary in a few fields like Primary Key
and accuracy, so combining the logic in one file makes sense.
"""


def outbrain_left_events(*columns):
    """
    Defines a source based off of the output table of the `base_table` StagingQuery
    """
    return Source(events=EventSource(
        isCumulative=True,
        table=get_staging_query_output_table_name(base_table),
        query=Query(
            selects=select(*columns),
            time_column="ts",
        ),
    ))


def outbrain_left_events_sampled(*columns):
    """
    Same as above, but sampled down to 10%
    2016-06-20	3,896,529
    2016-06-14	3,325,919
    2016-06-15	3651965
    2016-06-19	2997816
    2016-06-16	3661890
    2016-06-17	3518208
    2016-06-18	3090486

    2016-06-23	7117965
    2016-06-20	7,329,353
    2016-06-27	1198640
    2016-06-14	6,228,618
    2016-06-15	6784434
    2016-06-26	6272344
    2016-06-24	7044403
    2016-06-21	7145246
    2016-06-25	6078290
    2016-06-22	7146104
    2016-06-19	5547977
    2016-06-16	6878066
    2016-06-17	6623499
    2016-06-18	5746792
    """
    return Source(events=EventSource(
        isCumulative=True,
        table=get_staging_query_output_table_name(base_table),
        query=Query(
            selects=select(*columns),
            time_column="ts",
            wheres=["HASH(ad_id) % 100 < 10"]
        ),
    ))


def ctr_group_by(*keys, accuracy=Accuracy.TEMPORAL):
    """
    This is a helper function to create a GroupBy based on the source defined above, with fixed aggregations
    but variable Primary Keys accuracy.
    """
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
        accuracy=accuracy,
        isCumulative=True
    )


ad_doc = ctr_group_by("ad_id", "document_id", accuracy=Accuracy.SNAPSHOT)
ad_uuid = ctr_group_by("ad_id", "uuid")
"""
Here we are creating two GroupBys keyed off of ad_id, one with document_id as a secondary key and the
other with user id as it's secondary key.

The ad_doc group_by is set to SNAPSHOT accuracy, meaning feature values will be accurate as of midnight,
while the ad_uuid group_by is TEMPORAL accuracy, meaning values will be precisely correct, including intra-day updates.

The tradeoff is primarily that SNAPSHOT is cheaper to compute, while TEMPORAL is more accurate. Also, for
models that will eventually be online, you would want temporally accurate group_bys to be streaming (if offline
values are intra-day correct, online should be as well for consistency). In this case, the source is
the output of a staging query, which is always batch, so the user would need to convert the source to a streaming source,
as we did in the clicks_by_ad group_by.
"""


ad_platform = ctr_group_by("ad_id", "platform", "geo_location", accuracy=Accuracy.SNAPSHOT)
"""
Snapshot accuracy is a reasonable choice here because platform/geo is a very coarse grained aggregations,
so values are unlikely to meaningfully change intra day (midnight accuracy is sufficient)
"""