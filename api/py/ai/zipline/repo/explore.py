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
    "aggregation_base": [
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
    ]
}
GB_REL_PATH = "production/group_bys"
JOIN_REL_PATH = "production/joins"
FILTER_COLUMNS = ["aggregation_base", "keys"]

# colors chosen to be visible clearly on BOTH black and white terminals
# change with caution
BOLD = '\033[1m'
UNDERLINE = '\033[4m'
GREEN = '\033[38;5;28m'
ORANGE = '\033[38;5;130m'
NORMAL = '\033[0m'
BLUE = '\033[38;5;27m'
GREY = '\033[38;5;246m'


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


def build_entry(conf_path, index_spec):
    with open(conf_path) as conf_file:
        try:
            conf_json = json.load(conf_file)
            entry = {"file": conf_path}
            for column, paths in index_spec.items():
                result = []
                for path in paths:
                    result.extend(extract_json(path, conf_json))
                entry[column] = result
            return entry
        except BaseException as ex:
            print(f"Failed to parse {conf_path} due to :: {ex}")
            # traceback.print_exc()


def git_info(file_path):
    return subprocess.check_output(
        f"echo $(git log -n 1 --pretty='format:{GREY}on {GREEN}%as {GREY}by {BLUE}%an ' -- {file_path})",
        shell=True
    ).decode("utf-8").strip()


def walk_files(path):
    for root, _, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


def build_index(relative_path, index_spec):
    error_msg = "Either pass the path to index over, or run this script from the zipline root"
    assert CWD.endswith("zipline"), error_msg
    gb_path = os.path.join(CWD, relative_path)
    gb_meta_table = []
    for path in walk_files(gb_path):
        index_entry = build_entry(path, index_spec)
        if index_entry is not None:
            gb_meta_table.append(index_entry)
    return gb_meta_table


def prettify_entry(entry, target, show=10):
    lines = []
    modification = ""
    for column, values in entry.items():
        name = " "*(15 - len(column)) + column
        if column in FILTER_COLUMNS and len(values) > show:
            values = [value for value in set(values) if target in value]
            if(len(values) > show):
                truncated = ', '.join(values[:show])
                remaining = len(values) - show
                values = f"[{truncated} ... {GREY}{UNDERLINE}{remaining} more{NORMAL}]"
        if column == "file":
            rel_path = values[len(CWD)+1:]
            modification = git_info(values)
            values = f"{BOLD}{rel_path} {modification}{NORMAL}"
        lines.append(f"{BOLD}{ORANGE}{name}{NORMAL} - {values}")
    content = "\n" + "\n".join(lines)
    return (modification, content)


def find_in_index(index_table, target):
    def valid_entry(entry):
        return any([
            target in value.split("_")
            for column, values in entry.items()
            if column in FILTER_COLUMNS
            for value in values
        ])
    return [entry for entry in index_table if valid_entry(entry)]


def display_entries(entries, target):
    # sort by modification time and display
    sorted_entries = sorted(
        map(lambda e: prettify_entry(e, target), entries),
        key=lambda pr: pr[0], reverse=True
    )

    for (_, pretty_entry) in sorted_entries:
        print(pretty_entry)


def enrich_with_joins(group_bys, join_index):
    for group_by in group_bys:
        group_by["joins"] = []
        for join in join_index:
            if group_by["name"][0] in join["group_bys"]:
                group_by["joins"].append(join["name"][0])
        yield group_by


if __name__ == "__main__":
    gb_index = build_index(GB_REL_PATH, GB_INDEX_SPEC)
    join_index = build_index(JOIN_REL_PATH, JOIN_INDEX_SPEC)

    if len(sys.argv) != 2:
        print("This script takes just one argument, the keyword to lookup keys or features by")
        print("Eg., explore.py price")
        sys.exit(1)
    group_bys = find_in_index(gb_index, sys.argv[1])
    enriched = list(enrich_with_joins(group_bys, join_index))
    display_entries(enriched, sys.argv[1])
