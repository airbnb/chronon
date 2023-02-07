"""
Sample Label Join
"""
from sources import test_sources
from group_bys.sample_team import (
    event_sample_group_by,
    entity_sample_group_by_from_module,
    group_by_with_kwargs,
)

from ai.chronon.join import Join, JoinPart, LabelPart
from ai.chronon.group_by import GroupBy

label_part_group_by = GroupBy(
    name="sample_label_group_by",
    sources=test_sources.entity_source,
    keys=["group_by_subject"],
    aggregations=None,
    online=False,
)

v1 = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=group_by_with_kwargs.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
    ],
    label_part=LabelPart([
            JoinPart(
                group_by=label_part_group_by
            ),
        ],
        leftStartOffset=30,
        leftEndOffset=10),
    additional_args={
        'custom_arg': 'custom_value'
    },
    additional_env={
        'custom_env': 'custom_env_value'
    },
    online=False
)
