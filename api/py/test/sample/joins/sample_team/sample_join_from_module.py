"""
Sample Join
"""
from sources import test_sources
from ai.zipline.join import Join, JoinPart
from group_bys.sample_team import (
    sample_group_by_from_module,
    entity_sample_group_by_from_module,
)


v1 = Join(
    left = test_sources.staging_entities,
    right_parts=[
        JoinPart(
            group_by=sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        )
    ],
    additional_args={
        'custom_arg': 'custom_value'
    },
    additional_env={
        'custom_env': 'custom_env_value'
    },
)
