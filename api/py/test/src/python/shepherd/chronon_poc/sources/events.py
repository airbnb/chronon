from ai.chronon.query import (
    Query,
    select,
)
from ai.chronon.api import ttypes

merchant_card_right_part = ttypes.Source(events=ttypes.EventSource(
    table="sample_namespace.sample_table_group_by",
    query=Query(
        selects=select(
            merchant="merchant",
            card="card",
            amount="amount",
        ),
        start_partition="20230828",
        time_column="ts",
    ),
))

merchant_card_left_part = ttypes.Source(events=ttypes.EventSource(
    table="sample_namespace.sample_table_group_by",
    query=Query(
        selects=select(
            merchant="merchant",
            card="card",
            amount="amount",
        ),
        start_partition="20230828",
        time_column="ts",
    ),
))