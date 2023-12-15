
#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from ai.chronon.join import Join, JoinPart
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select

from group_bys.quickstart.purchases import v1 as purchases_v1
from group_bys.quickstart.returns import v1 as returns_v1
from group_bys.quickstart.users import v1 as users

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts", 
        query=Query(
            selects=select("user_id"), # The primary key used to join various GroupBys together
            time_column="ts",
            ) # The event time used to compute feature values as-of
    ))

v1 = Join(  
    left=source,
    right_parts=[JoinPart(group_by=group_by) for group_by in [purchases_v1, returns_v1, users]] # Include the three GroupBys
)

v2 = Join(
    left=source,
    right_parts=[JoinPart(group_by=group_by) for group_by in [purchases_v1, returns_v1]], # Include the two online GroupBys
    online=True,
)
