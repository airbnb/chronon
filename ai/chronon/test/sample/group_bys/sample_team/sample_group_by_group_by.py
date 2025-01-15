
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

from sources import test_sources
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Derivation
)
from ai.chronon import utils
from group_bys.sample_team.sample_group_by import require_backfill


v1 = GroupBy(
    sources=test_sources.basic_event_source(utils.group_by_output_table_name(require_backfill, True)),
    keys=["s2CellId", "place_id"],
    aggregations=[
        Aggregation(input_column="impressed_unique_count_1d_sum", operation=Operation.LAST),
    ],
    production=False,
    backfill_start_date="2022-01-01",
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    output_namespace="sample_namespace",
)
