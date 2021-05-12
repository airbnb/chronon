"""
Sample Non Production Join
"""
from ai.zipline.api import ttypes as api
from ai.zipline.join import join, join_part
from ai.zipline.utils import get_staging_query_output_table_name
from staging_queries.sample_team import sample_staging_query
from group_bys.sample_team import (
    sample_non_prod_group_by,
)


v1 = join(
    left = api.Source(
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
        join_part(
            group_by=sample_non_prod_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
    ],
    online=True,
)
