from ai.zipline.repo import teams
import pytest


def test_existence(teams_json):
    assert not teams.team_exists(teams_json, "Non_existing_team")
    assert teams.team_exists(teams_json, "sample_team")
    with pytest.raises(ValueError):
        teams.get_team_conf(teams_json, "Non_existing_team", "name")
