"""
Sample Join
"""
from sources import test_sources
from group_bys.sample_team import (
    event_sample_group_by,
    entity_sample_group_by_from_module,
    group_by_with_kwargs,
)

from ai.chronon.join import Join, JoinPart, BootstrapPart
from ai.chronon.query import Query, select
from ai.chronon.utils import get_join_output_table_name, get_staging_query_output_table_name

v1_join_parts = [
    JoinPart(
        group_by=event_sample_group_by.v1,
        key_mapping={'subject': 'group_by_subject'},
    ),
    JoinPart(
        group_by=entity_sample_group_by_from_module.v1,
        key_mapping={'subject': 'group_by_subject'},
    ),
]

v2_join_parts = [
    JoinPart(
        group_by=group_by_with_kwargs.v1,
        key_mapping={'subject': 'group_by_subject'},
    ),
]

v1 = Join(
    left=test_sources.event_source,
    right_parts=v1_join_parts,
    online=True,
    sample_percent=100.0,
    row_ids=["request_id"],
    bootstrap_from_log=True,
    bootstrap_parts=[
        BootstrapPart(
            "chronon_db.test_bootstrap_table",
            key_columns=["request_id"],
            query=Query(
                start_partition='2022-01-01',
                end_partition='2022-02-01',
                selects=select(field_a="field_a", field_b="field_b"),
            )
        )
    ]
)

v2 = Join(
    left=test_sources.event_source,
    right_parts=v1_join_parts + v2_join_parts,
    online=True,
    sample_percent=100.0,
    row_ids=["request_id"],
    bootstrap_from_log=True,
    bootstrap_parts=[
        BootstrapPart(
            table=get_join_output_table_name(v1, full_name=True)
        )
    ]
)
