"""A module used for reading teams.json file.
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

import json

# `default` team in teams.json contains default values.
DEFAULT_CONF_TEAM = 'default'

loaded_jsons = {}


def read_conf_json(json_path):
    if json_path not in loaded_jsons:
        with open(json_path) as w:
            team_json = json.load(w)
            loaded_jsons[json_path] = team_json
    return loaded_jsons[json_path]


def team_exists(json_path, team):
    team_json = read_conf_json(json_path)
    return team in team_json


def get_team_conf(json_path, team, key):
    team_json = read_conf_json(json_path)
    if team not in team_json:
        raise ValueError("team {} does not exist in {}".format(team, json_path))
    team_dict = team_json[team]
    if key in team_dict:
        return team_dict[key]
    else:
        return team_json[DEFAULT_CONF_TEAM][key]
