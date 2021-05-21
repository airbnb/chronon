import ai.zipline.api.ttypes as api
from ai.zipline.join import JoinPart, Join
from ai.zipline.query import Query, select


v1 = Join(
    left = api.Source(
        events=api.EventSource(
            table="sample_namespace.sample_table",
            query=api.Query(
                startPartition='2021-03-01',
                selects={
                    'subject': 'subject_expr',
                    'event': 'event_expr',
                },
                timeColumn="UNIX_TIMESTAMP(ts) * 1000"
            ),
        ),
    ),
    right_parts=[],
    dependencies=["sample_namespace.sample_table/ds={{ ds }}"]
)
