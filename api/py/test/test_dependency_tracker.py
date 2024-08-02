"""
Test dependency tracker
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

import pytest

from ai.chronon.repo import dependency_tracker


@pytest.fixture
def dependency_tracker():
    return dependency_tracker.ChrononRepoValidator(
        chronon_root_path='test/sample'
    )


def track_group_by_dependency(dependency_tracker):
    conf_path = 'group_bys/sample_team/sample_group_by.v1'
    downstream = dependency_tracker.check_downstream(conf_path)
    assert(downstream == ['sample_team.sample_join.v1'])


