# from airbnb.data_sources_2 import HiveEventSource
from ai.chronon.api import ttypes
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, select
from group_bys.unit_test.user.sample_nested_group_by import v1 as nested_v1

source = ttypes.Source(
    events=ttypes.EventSource(
        table="item_snapshot",
        query=Query(
            selects=select(user="id_requester"),
            wheres=["dim_requester_user_role = 'user'"],
            start_partition="2023-03-01",
            time_column="UNIX_TIMESTAMP(ts_created_at_utc) * 1000",
        ),
    )
)

v1 = Join(
    left=source,
    right_parts=[JoinPart(group_by=nested_v1)],
    online=True,
)
