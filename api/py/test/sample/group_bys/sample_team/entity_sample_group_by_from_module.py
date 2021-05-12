"""
Sample group by
"""
from ai.zipline.api import ttypes as api
from ai.zipline.group_by import group_by


v1 = group_by(
    sources=api.Source(
        entities=api.EntitySource(
            snapshotTable="sample_table.sample_entity_snapshot",
            query=api.Query(
                startPartition='2021-03-01',
                selects={
                    'group_by_subject': 'group_by_subject_expr',
                    'entity': 'entity_expr',
                },
                timeColumn="ts"
            ),
        ),
    ),
    keys=["group_by_subject"],
    aggregations=[
        api.Aggregation(
            inputColumn="entity",
            operation=api.Operation.LAST
        ),
        api.Aggregation(
            inputColumn="entity",
            operation=api.Operation.LAST,
            windows=[api.Window(length=7, timeUnit=api.TimeUnit.DAYS)],
        ),
    ],
)
