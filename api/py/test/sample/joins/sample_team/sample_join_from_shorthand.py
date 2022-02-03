from ai.zipline.join import JoinPart, Join
from ai.zipline.query import Query, select
from sources import test_sources


v1 = Join(
    left = test_sources.entity_source,
    right_parts=[],
)
