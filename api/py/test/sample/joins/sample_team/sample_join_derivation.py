"""
Sample Join
"""
from sources import test_sources
from group_bys.sample_team import (
    event_sample_group_by,
    entity_sample_group_by_from_module,
    group_by_with_kwargs,
)

from ai.chronon.join import Join, JoinPart, Derivation


v1 = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        )
    ],
    derivations=[
        Derivation(
            name="derived_field",
            expression="sample_team_event_sample_group_by_v1_event_sum_7d / sample_team_entity_sample_group_by_from_module_v1_entity_last"
        ),
        Derivation(
            name="*",
            expression="*"
        )
    ]
)
