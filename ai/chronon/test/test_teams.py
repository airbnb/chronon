
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

from ai.chronon.repo import teams
import pytest


def test_existence(teams_json):
    assert not teams.team_exists(teams_json, "Non_existing_team")
    assert teams.team_exists(teams_json, "sample_team")
    with pytest.raises(ValueError):
        teams.get_team_conf(teams_json, "Non_existing_team", "name")
