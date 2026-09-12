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

import os

import pytest
from ai.chronon.repo.explore import (
    GB_INDEX_SPEC,
    JOIN_INDEX_SPEC,
    build_entry,
    build_index,
    display_entries,
    enrich_with_joins,
    find_in_index,
    load_team_data,
)


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


@pytest.mark.parametrize("invalid_group_by", [{}, {"metaData": {"name": "invalid"}}])
def test_enrich_with_joins_skips_invalid_nested_group_bys(tmp_path, invalid_group_by):
    valid_group_by = {
        "metaData": {"name": "sample_team.valid.v1", "outputNamespace": "test_namespace"},
        "sources": [{"events": {"table": "source_events"}}],
    }
    join = {
        "metaData": {"name": "sample_team.join.v1", "outputNamespace": "test_namespace"},
        "left": {"events": {"table": "join_events"}},
        "joinParts": [{"groupBy": invalid_group_by}, {"groupBy": valid_group_by}],
    }
    join_entry = build_entry(join, JOIN_INDEX_SPEC, "joins", root=str(tmp_path))
    gb_index = {}

    enrich_with_joins(gb_index, {"sample_team.join.v1": join_entry}, root=str(tmp_path))

    assert list(gb_index) == ["sample_team.valid.v1"]
    assert gb_index["sample_team.valid.v1"]["sources"] == ["source_events"]
    assert gb_index["sample_team.valid.v1"]["joins"] == ["sample_team.join.v1"]
    assert gb_index["sample_team.valid.v1"]["join_event_driver"] == ["join_events"]
