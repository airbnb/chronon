"""
Sample Join
"""
from src.python.shepherd.chronon_poc.sources import events
from src.python.shepherd.chronon_poc.group_bys.sample_team import sample_group_by
from ai.chronon.join import (
    Join,
    JoinPart,
)


v1 = Join(
    left=events.merchant_card_left_part,
    right_parts=[JoinPart(group_by=sample_group_by.v1)],
    table_properties={
    },
    output_namespace="sample_namespace",
)
