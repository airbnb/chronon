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
from group_bys.sample_team import sample_group_by, sample_group_by_group_by
from ai.chronon.join import (
    Join,
    JoinPart,
)


v1 = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
    table_properties={
        "config_json": """{"sample_key": "sample_value"}"""
    },
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    env={
        "backfill": {
            "EXECUTOR_MEMORY": "9G"
        },
    },
)

never = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    offline_schedule='@never',
)

group_by_of_group_by = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by_group_by.v1)],
    output_namespace="sample_namespace",
)

consistency_check = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    check_consistency=True,
)

no_log_flattener = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1, tags={"experimental": True})],
    output_namespace="sample_namespace",
    tags={"business_relevance": "personalization"},
    sample_percent=0.0,
)
