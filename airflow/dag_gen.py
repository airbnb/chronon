import json
import os
import re


def normalize_object_name(object_name):
    """Normalize object(join, staging_query, group_by) name.
       to remove ds=XXXX-YY-ZZ out of paths
    """
    time_parts = ["ds", "ts", "hr"]

    def safe_part(p):
        return not any([
            p.startswith("{}=".format(time_part))
            for time_part in time_parts
        ])
    safe_name = "__".join(filter(safe_part, object_name.split("/")))
    return re.sub("[^A-Za-z0-9_]", "__", safe_name)


def walk_team_files_dir(root_dir):
    """
    This method walks configuration directories for each team
    returns {team: list(files)} dictionary.
    """
    team_dirs = [d for d in os.listdir(root_dir)
                 if os.path.isdir(os.path.join(root_dir, d))]
    result = {}
    for team in team_dirs:
        files = []
        for sub_root, sub_dirs, sub_files in os.walk(
                os.path.join(root_dir, team)):
            for f in sub_files:
                files.append(os.path.join(sub_root, f))
        result[team] = files
    return result


def get_json_dict(conf):
    """
    get json dict regardless of its input -  a path to a json file, a string json, or json as a dict
    """
    if isinstance(conf, dict):
        return conf

    conf_json_dict = conf
    # file path could be str type or unicode type
    if ((isinstance(conf, six.text_type) or isinstance(conf, six.string_types)) and ' ' not in conf) and os.path.exists(conf):
        with open(conf) as conf_file:
            conf_json_dict = json.load(conf_file)
    elif isinstance(conf, six.text_type) or isinstance(conf, six.string_types):
        conf_json_dict = json.loads(conf)
    return conf_json_dict


def json_extract(conf, nesting, default):
    """
    conf    - either a path to a json file, a string json, or json as a dict
    nesting - is a `.` separated list of strings.

            - `$` is used to parse a nested string as json
            - eg., 'metaData.customJson.$.lag'
            - (indicates that customJson is stored as stringified json)

            - `|` is a fork separator.
            - eg., "left.entities|events.query"
            - Pick the first non-null nested value
    """
    curr = get_json_dict(conf)
    nests = nesting.split('.')
    for nest in nests:
        if nest == '$':
            curr = json.loads(curr)
        elif '|' in nest:
            fork_val = {}
            for fork in nest.split('|'):
                if fork in curr:
                    fork_val = curr.get(fork)
                    break
            curr = fork_val
        else:
            curr = curr.get(nest, {})
        if curr == {} or None:
            print("Failed at parsing `{}` in `{}` from {}".format(
                nest, nesting, conf))
    return default if curr == {} or curr is None else curr