"""
Run the flow for materialize.
"""
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
