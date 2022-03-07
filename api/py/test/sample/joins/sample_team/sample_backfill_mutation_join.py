"""
Sample entity temporal accurate backfill (mutation)
"""
from sources import test_sources
from group_bys.sample_team import mutation_sample_group_by
from ai.zipline.join import Join, JoinPart


v0 = Join(
    left=test_sources.event_source,
    right_parts=[JoinPart(group_by=mutation_sample_group_by.v0)],
    online=False,
)

