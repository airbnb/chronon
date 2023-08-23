"""
Sample Chaining Group By with join as a source
"""
from sources import test_sources
from group_bys.sample_team import (
    event_sample_group_by,
    entity_sample_group_by_from_module,
    group_by_with_kwargs,
)

from ai.chronon.join import Join, JoinPart
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Accuracy,
    Operation,
)
from ai.chronon.api import ttypes
from ai.chronon.query import (
    Query,
    select,
)

parent_join = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
    ],
    online=True,
    check_consistency=True
)

chaining_group_by_v1 = GroupBy(
    sources=ttypes.Source(joinSource=ttypes.JoinSource(
        join=parent_join,
        query=Query(
            selects=select(
                event="event_expr",
                group_by_subject="group_by_expr",
            ),
            start_partition="2023-04-15",
            time_column="ts",
        ))),
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.LAST),
    ],
    accuracy=Accuracy.TEMPORAL,
    online=True,
    production=True,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    output_namespace="sample_namespace",
)
