from ai.chronon.join import JoinPart, Join
from ai.chronon.query import Query, select
from sources import test_sources


v1 = Join(
    left = test_sources.entity_source,
    right_parts=[],
)
