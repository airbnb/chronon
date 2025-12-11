"""
Sample Join
"""

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

from ai.chronon.join import Derivation, Join, JoinPart
from ai.chronon.query import (
    Query,
    select,
)
from ai.chronon.api import ttypes
from group_bys.sample_team import (
    entity_sample_group_by_from_module,
    event_sample_group_by,
)

# Sample Event Source used in tests.
left_part = ttypes.Source(events=ttypes.EventSource(
    table="sample_namespace.sample_table_group_by",
    query=Query(
        selects=select(
            event="event_expr",
            subject="group_by_subject_expr",
        ),
        start_partition="2021-04-09",
        time_column="ts",
    ),
))


v1 = Join(
    left=left_part,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
    ],
    derivations=[
        Derivation(
            name="derived_field",
            expression="sample_team_event_sample_group_by_v1_event_sum_7d / sample_team_entity_sample_group_by_from_module_v1_entity_last",
        ),
        Derivation(name="event_sum_7d", expression="sample_team_event_sample_group_by_v1_event_sum_7d"),
        Derivation(name="event_sum", expression="sample_team_event_sample_group_by_v1_event_sum"),
        Derivation(name="event_approx_percentile", expression="sample_team_event_sample_group_by_v1_event_approx_percentile"),
        Derivation(name="entity_last", expression="sample_team_entity_sample_group_by_from_module_v1_entity_last"),
        Derivation(name="entity_last_7d", expression="sample_team_entity_sample_group_by_from_module_v1_entity_last_7d"),
    ],
)
