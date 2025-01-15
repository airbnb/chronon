from ai.chronon.group_by import GroupBy, Aggregation, Operation
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select


logging_schema_source = Source(
    events=EventSource(
        table="default.chronon_log_table",
        query=Query(
            selects=select(
                schema_hash="decode(unbase64(key_base64), 'utf-8')",
                schema_value="decode(unbase64(value_base64), 'utf-8')"
            ),
            wheres=["name='SCHEMA_PUBLISH_EVENT'"],
            time_column="ts_millis",
        ),
    )
)

v1 = GroupBy(
    keys=["schema_hash"],
    sources=logging_schema_source,
    aggregations=[
        Aggregation(
            input_column="schema_value",
            operation=Operation.LAST
        )
    ],
    online=False,
    backfill_start_date="2023-04-09"
)
