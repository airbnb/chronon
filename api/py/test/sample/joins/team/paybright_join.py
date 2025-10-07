from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.join import Join, JoinPart
from ai.chronon.group_by import GroupBy

# Import the GroupBys
from group_bys.team.paybright_repayment_data import paybright_repayment_data_v1
from group_bys.team.paybright_user_features import paybright_user_features_v1

# 1. Create the left side source (checkout events)
checkout_source = Source(
    events=EventSource(
        table="checkouts",  # Your checkout events table
        query=Query(
            selects=select("user_id"),  # Primary key
            time_column="ts",  # Event timestamp
        ),
    ),
)

# 2. Create the Join
paybright_join_v1 = Join(
    left=checkout_source,
    right_parts=[
        JoinPart(group_by=paybright_repayment_data_v1),
        JoinPart(group_by=paybright_user_features_v1),
    ],
)

print("Join created successfully!")
