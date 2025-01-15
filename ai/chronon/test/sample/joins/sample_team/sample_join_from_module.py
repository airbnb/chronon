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

from sources import test_sources
from ai.chronon.join import Join, JoinPart
from group_bys.sample_team import (
    sample_group_by_from_module,
    entity_sample_group_by_from_module,
)


v1 = Join(
    left = test_sources.staging_entities,
    right_parts=[
        JoinPart(
            group_by=sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        )
    ],
    additional_args={
        'custom_arg': 'custom_value'
    },
    additional_env={
        'custom_env': 'custom_env_value'
    },
)
