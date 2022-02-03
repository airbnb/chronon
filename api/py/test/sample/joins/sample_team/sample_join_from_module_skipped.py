"""
Sample Non Production Join
"""
from sources import test_sources
from group_bys.sample_team import sample_non_prod_group_by

from ai.zipline.join import Join, JoinPart


v1 = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=sample_non_prod_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
    ],
    online=True,
)
