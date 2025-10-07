from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.join import Join, JoinPart
from ai.chronon.group_by import GroupBy

# Import the GroupBys
from group_bys.affirm.paybright.repayment_data_v1_0 import paybright_repayment_data_v1_0
from group_bys.affirm.instrument.instrument_risk_data_v1_0 import instrument_risk_data_v1_0

# 1. Create the left side source (checkout events)
checkout_source = Source(
    events=EventSource(
        table="checkouts",  # Your checkout events table
        query=Query(
            selects=select("user_phone_number", "user_ari"),  # Both keys needed
            time_column="ts",  # Event timestamp
        ),
    ),
)

# 2. Create the Join with both GroupBys
paybright_join_v1_0 = Join(
    left=checkout_source,
    right_parts=[
        JoinPart(group_by=paybright_repayment_data_v1_0),
        JoinPart(group_by=instrument_risk_data_v1_0),
    ],
)

print("Join created successfully!")
