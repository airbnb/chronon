"""A module used for reading teams.json file.
"""
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
