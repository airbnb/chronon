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

    :param table:
    :param topic:
    :param query:
    :param is_cumulative:
    :return:
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

    :param snapshot_table:
    :param mutation_table:
    :param query:
    :param mutation_topic:
    :return:
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

    :param join:
    :param query:
    :return:
    """
    return ttypes.Source(
        joinSource=ttypes.JoinSource(
            join=join,
            query=query
        )
    )
