import json
from typing import Optional

import ai.chronon.api.ttypes as ttypes


def EventSource(
    table: str,
    query: ttypes.Query,
    topic: Optional[str] = None,
    is_cumulative: Optional[bool] = None,
    **kwargs
) -> ttypes.Source:
    """

    :param table: Points to a table that has historical data for the input events
    :param query: Contains row level transformations and filtering expressed as Spark SQL statements. Applied to both table and topic
    :param topic: (Optional) Kafka topic that can be listened to for realtime updates
    :param is_cumulative: Indicates that each new partition contains not just the current day's events but the entire set of events since the beginning
    :return:
      A source object of kind EventSource
    """
    return ttypes.Source(
        events=ttypes.EventSource(
            table=table,
            topic=topic,
            query=query,
            isCumulative=is_cumulative,
            customJson=json.dumps(kwargs),
        )
    )


def EntitySource(
    snapshot_table: str,
    query: ttypes.Query,
    mutation_table: Optional[str] = None,
    mutation_topic: Optional[str] = None,
    **kwargs
) -> ttypes.Source:
    """

    :param snapshot_table: Points to a table that contains periodical snapshots of the entire dataset
    :param query: Contains row level transformations and filtering expressed as Spark SQL statements
    :param mutation_table: (Optional) Points to a table that contains all changes applied to the dataset
    :param mutation_topic: (Optional) Kafka topic that delivers changes in realtime
    :return:
      A source object of kind EntitySource
    """
    return ttypes.Source(
        entities=ttypes.EntitySource(
            snapshotTable=snapshot_table,
            mutationTable=mutation_table,
            query=query,
            mutationTopic=mutation_topic,
            customJson=json.dumps(kwargs),
        )
    )


def JoinSource(
    join: ttypes.Join,
    query: ttypes.Query,
) -> ttypes.Source:
    """

    :param join: Output of downstream Join operation
    :param query: Contains row level transformations and filtering expressed as Spark SQL statements
    :return:
      A source object of kind JoinSource
    """
    return ttypes.Source(
        joinSource=ttypes.JoinSource(
            join=join,
            query=query
        )
    )
