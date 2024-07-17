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

from ai.chronon.group_by import Aggregation, GroupBy, Operation
from sources import test_sources

v1 = GroupBy(
    sources=test_sources.staging_entities,
    keys=["s2CellId", "place_id"],
    aggregations=[
        Aggregation(operation=Operation.COUNT),
        Aggregation(operation=Operation.COUNT),
    ],
    production=False,
    table_properties={"sample_config_json": """{"sample_key": "sample_value"}""", "description": "sample description"},
    online=True,
    output_namespace="sample_namespace",
)
