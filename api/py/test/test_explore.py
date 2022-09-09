"""
Run the flow for materialize.
"""
from ai.chronon.repo.explore import (
    load_team_data,
    build_index,
    enrich_with_joins,
    display_entries,
    find_in_index,
    parse_args,
    main,
    GB_INDEX_SPEC,
    JOIN_INDEX_SPEC,
)

import pytest


@pytest.mark.parametrize("keyword", ["event", "entity"])
def test_basic_flow(teams_json, sampledir, keyword):
    teams = load_team_data(teams_json)
    gb_index = build_index("group_bys", GB_INDEX_SPEC, root=sampledir, teams=teams)
    join_index = build_index("joins", JOIN_INDEX_SPEC, root=sampledir, teams=teams)
    enrich_with_joins(gb_index, join_index, root=sampledir, teams=teams)
    group_bys = find_in_index(gb_index, keyword)
    display_entries(group_bys, keyword, root=sampledir, trim_paths=True)
    assert len(group_bys) > 0


@pytest.mark.parametrize("keyword", ["event", "entity"])
def test_main(sampledir, keyword):
    args = parse_args([keyword, "--conf-root", sampledir])
    main(args)


def test_handlers(sampledir):
    args = parse_args(["_events_without_topics", "--conf-root", sampledir])
    main(args)
