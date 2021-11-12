"""
Sample Non Production Join
"""
from group_bys.sample_team import (
    sample_non_prod_group_by,
)

from ai.zipline.api import ttypes as api
from ai.zipline.join import Join, JoinPart

v1 = Join(
    left=api.Source(
        events=api.EventSource(
            table="sample_namespace.sample_table_skipped",
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
    right_parts=[
        JoinPart(
            group_by=sample_non_prod_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
    ],
    online=True,
)
