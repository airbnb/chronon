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
    Window,
    TimeUnit,
    Derivation
)

v1 = GroupBy(
    sources=test_sources.staging_entities,
    keys=["s2CellId", "place_id"],
    aggregations=[
        Aggregation(input_column="impressed_unique_count", operation=Operation.SUM),
        Aggregation(input_column="viewed_unique_count", operation=Operation.SUM),
    ],
    production=False,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    derivations=[
        Derivation(
            name="impressed_unique_count_1d_new_name",
            expression="impressed_unique_count_sum"
        )
    ],
    output_namespace="sample_namespace",
)
