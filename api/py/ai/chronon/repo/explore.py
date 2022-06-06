#!/usr/bin/env python3

import json
import os
import subprocess
import sys


CWD = os.getcwd()
GB_INDEX_SPEC = {
    "sources": [
        "sources[].events.table",
        "sources[].entities.snapshotTable",
        "sources[].entities.mutationTable",
        "sources[].entities.topic",
        "sources[].events.topic",
    ],
    "aggregation": [
        "aggregations[].inputColumn"
    ],
    "keys": [
        "keyColumns"
    ],
    "name": [
        "metaData.name",
        "name"
    ]
}

JOIN_INDEX_SPEC = {
    "input_table": [
        "left.entities.snapshotTable",
        "left.events.table",
    ],
    "group_bys": [
        "joinParts[].groupBy.metaData.name",
        "rightParts[].groupBy.name",
    ],
    "name": [
        "metaData.name",
        "name"
    ],
    "_group_bys": [
        "joinParts[].groupBy",
        "rightParts[].groupBy"
    ]
}
GB_REL_PATH = "production/group_bys"
JOIN_REL_PATH = "production/joins"
FILTER_COLUMNS = ["aggregation", "keys", "name", "sources", "joins"]

# colors chosen to be visible clearly on BOTH black and white terminals
# change with caution
NORMAL = '\033[0m'
BOLD = '\033[1m'
ITALIC = '\033[3m'
UNDERLINE = '\033[4m'
RED = '\033[38;5;160m'
GREEN = '\033[38;5;28m'
ORANGE = '\033[38;5;130m'
BLUE = '\033[38;5;27m'
GREY = '\033[38;5;246m'
HIGHLIGHT = BOLD+ITALIC+RED


# walks the json nodes recursively collecting all values that match the path
# a trailing `[]` in a field in the path indicates that there is an array of
# object in the correspoding node value.
def extract_json(json_path, conf_json):
    if json_path is None:
        return conf_json
    steps = json_path.split(".", 1)
    key = steps[0]
    next = steps[1] if len(steps) > 1 else None
    if key.endswith("[]"):
        key = key[:-2]
        if key in conf_json:
            result = []
            for value in conf_json[key]:
                result.extend(extract_json(next, value))
            return result
    else:
        if key in conf_json:
            final = extract_json(next, conf_json[key])
            if isinstance(final, list):
                return final
            else:
                return [final]
    return []


def build_entry(conf, index_spec, conf_type):
    conf_dict = conf
    if isinstance(conf, str):
        with open(conf) as conf_file:
            try:
                conf_dict = json.load(conf_file)
            except BaseException as ex:
                print(f"Failed to parse {conf} due to :: {ex}")
                return
    entry = {"file": None}
    for column, paths in index_spec.items():
        result = []
        for path in paths:
            result.extend(extract_json(path, conf_dict))
        entry[column] = result

    # derive python file path from the name & conf_type
    (team, conf_module) = entry["name"][0].split(".", 1)
    file_base = "/".join(conf_module.split(".")[:-1])
    py_file = file_base + ".py"
    init_file = file_base + "/__init__.py"
    py_path = os.path.join(conf_type, team, py_file)
    init_path = os.path.join(conf_type, team, init_file)
    conf_path = py_path if os.path.exists(py_path) else init_path
    entry["file"] = conf_path
    return entry


# git_info is the most expensive part of the entire script - so we will have to parallelize
def git_info(file_paths):
    procs = []
    for file_path in file_paths:
        args = f"echo $(git log -n 1 --pretty='format:{GREY}on {GREEN}%as {GREY}by {BLUE}%an ' -- {file_path})"
        procs.append((file_path, subprocess.Popen(args, stdout=subprocess.PIPE, shell=True)))

    result = {}
    for file_path, proc in procs:
        lines = []
        for line in proc.stdout.readlines():
            lines.append(line.decode("utf-8").strip())
        result[file_path] = lines[0]
    return result


def walk_files(path):
    for root, _, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


def build_index(conf_type, index_spec):
    rel_path = os.path.join(CWD, "production", conf_type)
    index_table = {}
    for path in walk_files(rel_path):
        index_entry = build_entry(path, index_spec, conf_type)
        if index_entry is not None:
            index_table[index_entry["name"][0]] = index_entry
    return index_table


def find_string(text, word):
    start = text.find(word)
    while start > -1:
        yield start
        start = text.find(word, start + 1)


def highlight(text, word):
    result = ""
    prev_idx = 0
    for idx in find_string(text, word):
        result = result + text[prev_idx:idx] + HIGHLIGHT + word + NORMAL
        prev_idx = idx + len(word)
    result += text[prev_idx:len(text)]
    return result


def prettify_entry(entry, target, modification, show=10):
    lines = []
    for column, values in entry.items():
        name = " "*(15 - len(column)) + column
        if column in FILTER_COLUMNS and len(values) > show:
            values = [value for value in set(values) if target in value]
            if(len(values) > show):
                truncated = ', '.join(values[:show])
                remaining = len(values) - show
                values = f"[{truncated} ... {GREY}{UNDERLINE}{remaining} more{NORMAL}]"
        if column == "file":
            values = f"{BOLD}{values} {modification}{NORMAL}"
        else:
            values = highlight(str(values), target)
        lines.append(f"{BOLD}{ORANGE}{name}{NORMAL} - {values}")
    content = "\n" + "\n".join(lines)
    return content


def find_in_index(index_table, target):
    def valid_entry(entry):
        return any([
            target in value
            for column, values in entry.items()
            if column in FILTER_COLUMNS
            for value in values
        ])
    return [entry for _, entry in index_table.items() if valid_entry(entry)]


def display_entries(entries, target):
    git_infos = git_info([entry["file"] for entry in entries])
    display = []
    for entry in entries:
        info = git_infos[entry["file"]]
        pretty = prettify_entry(entry, target, info)
        display.append((info, pretty))

    for (_, pretty_entry) in sorted(display):
        print(pretty_entry)


def enrich_with_joins(gb_index, join_index):
    # nested gb entries
    for _, join_entry in join_index.items():
        for gb in join_entry["_group_bys"]:
            entry = build_entry(gb, GB_INDEX_SPEC, "group_bys")
            gb_index[entry["name"][0]] = entry
    # lineage -> reverse index from gb -> join
    for _, group_by in gb_index.items():
        group_by["joins"] = []
    for _, join in join_index.items():
        for gb_name in join["group_bys"]:
            if gb_name in gb_index:
                gb_index[gb_name]["joins"].append(join["name"][0])


if __name__ == "__main__":
    if not CWD.endswith("chronon"):
        print("This script needs to be run from chronon conf root - with folder named 'chronon'")
    gb_index = build_index("group_bys", GB_INDEX_SPEC)
    join_index = build_index("joins", JOIN_INDEX_SPEC)
    enrich_with_joins(gb_index, join_index)
    if len(sys.argv) != 2:
        print("This script takes just one argument, the keyword to lookup keys or features by")
        print("Eg., explore.py price")
        sys.exit(1)
    group_bys = find_in_index(gb_index, sys.argv[1])
    display_entries(group_bys, sys.argv[1])
