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
def test_dependency_tracker():
    return dependency_tracker.ChrononEntityDependencyTracker(
        chronon_root_path='test/sample/production'
    )


def test_group_by_dependency(test_dependency_tracker):
    conf_path = 'group_bys/sample_team/event_sample_group_by.v1'
    downstream = test_dependency_tracker.check_downstream(conf_path)
    assert (len(downstream) == 8)
    assert ('sample_team.sample_join_derivation.v1' in downstream)


def test_join_dependency(test_dependency_tracker):
    conf_path = 'joins/sample_team/sample_chaining_join.parent_join'
    downstream = test_dependency_tracker.check_downstream(conf_path)
    assert (len(downstream) == 1)
    assert ('sample_team.sample_chaining_group_by' in downstream)
