"""
Run the flow for materialize.
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

from ai.chronon.repo.explore import (
    load_team_data,
    build_index,
    enrich_with_joins,
    display_entries,
    find_in_index,
    GB_INDEX_SPEC,
    JOIN_INDEX_SPEC,
)

import pytest
import os


@pytest.mark.parametrize("keyword", ["event", "entity"])
def test_basic_flow(teams_json, rootdir, keyword):
    teams = load_team_data(teams_json)
    root = os.path.join(rootdir, "sample")
    gb_index = build_index("group_bys", GB_INDEX_SPEC, root=root, teams=teams)
    join_index = build_index("joins", JOIN_INDEX_SPEC, root=root, teams=teams)
    enrich_with_joins(gb_index, join_index, root=root, teams=teams)
    group_bys = find_in_index(gb_index, keyword)
    display_entries(group_bys, keyword, root=root, trim_paths=True)
    assert len(group_bys) > 0
