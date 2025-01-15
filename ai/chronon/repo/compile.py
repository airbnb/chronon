#!/usr/bin/env python
# tool to materialize feature_sources and feature_sets into thrift configurations
# that chronon jobs can consume


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
import logging
import os
import pprint
import textwrap
from typing import List, Optional, Tuple, Type, Union

import ai.chronon.api.ttypes as api
import ai.chronon.repo.extract_objects as eo
import ai.chronon.utils as utils
import click
from ai.chronon.api.ttypes import GroupBy, Join, StagingQuery
from ai.chronon.repo import (
    GROUP_BY_FOLDER_NAME,
    JOIN_FOLDER_NAME,
    STAGING_QUERY_FOLDER_NAME,
    TEAMS_FILE_PATH,
    dependency_tracker,
    teams,
)
from ai.chronon.repo.serializer import thrift_simple_json_protected
from ai.chronon.repo.validator import (
    ChrononRepoValidator,
    get_group_by_output_columns,
    get_join_output_columns,
)

# This is set in the main function -
# from command line or from env variable during invocation
FOLDER_NAME_TO_CLASS = {
    GROUP_BY_FOLDER_NAME: GroupBy,
    JOIN_FOLDER_NAME: Join,
    STAGING_QUERY_FOLDER_NAME: StagingQuery,
}

DEFAULT_TEAM_NAME = "default"


def get_folder_name_from_class_name(class_name):
    return {v.__name__: k for k, v in FOLDER_NAME_TO_CLASS.items()}[class_name]


@click.command()
@click.option("--chronon_root", envvar="CHRONON_ROOT", help="Path to the root chronon folder", default=os.getcwd())
@click.option(
    "--input_path",
    "--conf",
    "input_path",
    help="Relative Path to the root chronon folder, which contains the objects to be serialized",
    required=True,
)
@click.option(
    "--output_root",
    help="Relative Path to the root chronon folder, to where the serialized output should be written",
    default="production",
)
@click.option("--debug", help="debug mode", is_flag=True)
@click.option("--force-overwrite", help="Force overwriting existing materialized conf.", is_flag=True)
@click.option("--feature-display", help="Print out the features list created by the conf.", is_flag=True)
@click.option(
    "--table-display",
    help="Print out the list of tables that are materialized per job modes in this conf.",
    is_flag=True,
)
def extract_and_convert(chronon_root, input_path, output_root, debug, force_overwrite, feature_display, table_display):
    """
    CLI tool to convert Python chronon GroupBy's, Joins and Staging queries into their thrift representation.
    The materialized objects are what will be submitted to spark jobs - driven by airflow, or by manual user testing.
    """
    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    _print_highlighted("Using chronon root path", chronon_root)
    chronon_root_path = os.path.expanduser(chronon_root)
    utils.chronon_root_path = chronon_root_path
    path_split = input_path.split("/")
    obj_folder_name = path_split[0]
    obj_class = FOLDER_NAME_TO_CLASS[obj_folder_name]
    full_input_path = os.path.join(chronon_root_path, input_path)
    _print_highlighted(f"Input {obj_folder_name} from", full_input_path)
    assert os.path.exists(full_input_path), f"Input Path: {full_input_path} doesn't exist"
    if os.path.isdir(full_input_path):
        results = eo.from_folder(chronon_root_path, full_input_path, obj_class, log_level=log_level)
    elif os.path.isfile(full_input_path):
        assert full_input_path.endswith(".py"), f"Input Path: {input_path} isn't a python file"
        results = eo.from_file(chronon_root_path, full_input_path, obj_class, log_level=log_level)
    else:
        raise Exception(f"Input Path: {full_input_path}, isn't a file or a folder")
    validator = ChrononRepoValidator(chronon_root_path, output_root, log_level=log_level)
    extra_online_or_gb_backfill_enabled_group_bys = {}
    extra_dependent_group_bys_to_materialize = {}
    extra_dependent_joins_to_materialize = {}
    num_written_objs = 0
    full_output_root = os.path.join(chronon_root_path, output_root)
    teams_path = os.path.join(chronon_root_path, TEAMS_FILE_PATH)
    entity_dependency_tracker = dependency_tracker.ChrononEntityDependencyTracker(
        chronon_root_path=full_output_root,
        log_level=log_level,
    )

    _print_debug_info(results.keys(), f"Extracted Entities Of Type {obj_class.__name__}", log_level)

    for name, obj in results.items():
        team_name = name.split(".")[0]
        _set_team_level_metadata(obj, teams_path, team_name)
        _set_templated_values(obj, obj_class, teams_path, team_name)
        if _write_obj(full_output_root, validator, name, obj, log_level, force_overwrite, force_overwrite):
            num_written_objs += 1

            if feature_display:
                _print_features(obj, obj_class)

            if table_display:
                _print_tables(obj, obj_class)

            # In case of join, we need to materialize the following underlying group_bys
            # 1. group_bys whose online param is set
            # 2. group_bys whose backfill_start_date param is set.
            if obj_class is Join:
                online_group_bys = {}
                offline_backfill_enabled_group_bys = {}
                offline_gbs = []  # gather list to report errors
                for jp in obj.joinParts:
                    if jp.groupBy.metaData.online:
                        online_group_bys[jp.groupBy.metaData.name] = jp.groupBy
                    elif jp.groupBy.backfillStartDate:
                        offline_backfill_enabled_group_bys[jp.groupBy.metaData.name] = jp.groupBy
                    else:
                        offline_gbs.append(jp.groupBy.metaData.name)

                _print_debug_info(list(online_group_bys.keys()), "Online Groupbys", log_level)
                _print_debug_info(
                    list(offline_backfill_enabled_group_bys.keys()), "Offline Groupbys With Backfill Enabled", log_level
                )
                _print_debug_info(offline_gbs, "Offline Groupbys", log_level)
                extra_online_or_gb_backfill_enabled_group_bys.update(
                    {**online_group_bys, **offline_backfill_enabled_group_bys}
                )
                if obj.metaData.online:
                    group_bys_not_online = offline_gbs + list(offline_backfill_enabled_group_bys.keys())
                    assert not group_bys_not_online, (
                        "You must make all dependent GroupBys `online` if you want to make your join [{}] `online`."
                        " You can do this by passing the `online=True` argument to the GroupBy constructor."
                        " Fix the following: {}".format(obj.metaData.name, group_bys_not_online)
                    )

            new_group_bys, new_joins = _handle_dependent_configurations(
                chronon_root_path, entity_dependency_tracker, full_output_root, name, obj, obj_class, log_level
            )

            _handle_deprecation_warning(obj, obj_class, new_group_bys, new_joins)

            # Update the accumulated dictionaries with the new entries
            extra_dependent_group_bys_to_materialize.update(new_group_bys)
            extra_dependent_joins_to_materialize.update(new_joins)

    if not force_overwrite:
        dependencies = {}
        dependencies.update({**extra_dependent_group_bys_to_materialize, **extra_dependent_joins_to_materialize})
        assert not dependencies, (
            "You must also materialize all dependent GroupBys or Joins."
            " You can do this by passing the --force-overwrite flag to the compile.py command."
            " Detected dependencies are as follows: {}".format(sorted(dependencies.keys()))
        )

    if extra_online_or_gb_backfill_enabled_group_bys or extra_dependent_group_bys_to_materialize:
        extra_online_or_gb_backfill_enabled_group_bys.update(extra_dependent_group_bys_to_materialize)
        _handle_extra_conf_objects_to_materialize(
            conf_objs=extra_online_or_gb_backfill_enabled_group_bys,
            force_overwrite=force_overwrite,
            full_output_root=full_output_root,
            teams_path=teams_path,
            validator=validator,
            log_level=log_level,
        )
    if extra_dependent_joins_to_materialize:
        _handle_extra_conf_objects_to_materialize(
            conf_objs=extra_dependent_joins_to_materialize,
            force_overwrite=force_overwrite,
            full_output_root=full_output_root,
            teams_path=teams_path,
            validator=validator,
            log_level=log_level,
            is_gb=False,
        )
    if num_written_objs > 0:
        print(f"Successfully wrote {num_written_objs} {(obj_class).__name__} objects to {full_output_root}")


def _handle_dependent_configurations(
    chronon_root_path: str,
    entity_dependency_tracker: dependency_tracker.ChrononEntityDependencyTracker,
    full_output_root: str,
    name: str,
    obj: object,
    obj_class: Type[Union[Join, GroupBy]],
    log_level=logging.INFO,
) -> Tuple[dict, dict]:
    # Initialize local dictionaries to store new entries to be added
    new_group_bys_to_materialize = {}
    new_joins_to_materialize = {}

    output_file_path = _get_relative_materialized_file_path(full_output_root, name, obj)
    downstreams = entity_dependency_tracker.get_downstream_names(output_file_path)

    _print_debug_info(downstreams, f"Downstreams For {output_file_path}", log_level)

    for downstream in downstreams:
        if obj_class is Join:
            downstream_class = GroupBy
        elif obj_class is GroupBy:
            downstream_class = Join
        else:
            continue
        conf_result = _get_conf_file_path(downstream, downstream_class)
        if conf_result is None:
            if log_level == logging.DEBUG:
                print(f"No {downstream} dependency of type {downstream_class} for {name}")
            continue

        conf_var, conf_file_path = conf_result
        try:
            conf_obj = eo.from_file(
                chronon_root_path,
                os.path.join(chronon_root_path, conf_file_path),
                downstream_class,
                log_level=log_level,
            )
            _print_debug_info(
                [key for key, _ in conf_obj.items()],
                f"Entities Within Dependent Configuration To Compile {conf_file_path}",
                log_level,
            )
        except Exception as e:
            _print_error(
                f"Failed to parse {downstream} dependency of type {downstream_class} for {name} at {conf_file_path}",
                str(e),
            )
            raise e

        obj_to_materialize = conf_obj[downstream]
        if downstream_class is GroupBy:
            new_group_bys_to_materialize[obj_to_materialize.metaData.name] = obj_to_materialize
        elif downstream_class is Join:
            new_joins_to_materialize[obj_to_materialize.metaData.name] = obj_to_materialize

    # Return new entries to be added to the main dictionaries
    return new_group_bys_to_materialize, new_joins_to_materialize


def _get_relative_materialized_file_path(full_output_root: str, name: str, obj: object) -> str:
    _, _, abs_output_file_path = _construct_output_file_name(full_output_root, name, obj)
    output_file_path = abs_output_file_path.replace(full_output_root + os.sep, "")
    return output_file_path


def _get_conf_file_path(downstream: str, downstream_class: Type[Union[Join, GroupBy]]) -> Optional[Tuple[str, str]]:
    parts = downstream.split(".")

    # This may happen for scenarios where group_by is defined within join file itself.
    # So no separate group_by file is present. In such cases, compiling the join will handle the materialize
    # for group_by.
    if len(parts) < 3:
        return None

    team_name = parts[0]
    conf_var = parts[-1]
    conf_name = parts[-2] + ".py"

    # Nested path between team_name and conf_name
    nested_path = parts[1:-2] if len(parts) > 3 else []

    # Construct the path for configuration file
    conf_file_path = os.path.join(team_name, *nested_path, conf_name)

    # Return the appropriate path based on downstream_class
    if downstream_class is Join:
        return conf_var, os.path.join(JOIN_FOLDER_NAME, conf_file_path)
    elif downstream_class is GroupBy:
        return conf_var, os.path.join(GROUP_BY_FOLDER_NAME, conf_file_path)
    else:
        raise ValueError(f"Invalid downstream class: {downstream_class}")


def _print_features(obj: Union[Join, GroupBy], obj_class: Type[Union[Join, GroupBy]]) -> None:
    if obj_class is Join:
        features = get_join_output_columns(obj)
        for key, value in features.items():
            left = key.value.replace("_", " ").title()
            right = pprint.pformat(value, width=80, compact=True)
            _print_features_names(left, f"\n{right}")
    if obj_class is GroupBy:
        _print_features_names("Output GroupBy Features", get_group_by_output_columns(obj))


def check_deprecation_date_setup_for_downstream(collection, entity_type, obj_name, obj_deprecation_date):
    incorrect_deprecation_date = [
        name
        for name, item in collection.items()
        if not item.metaData.deprecationDate or item.metaData.deprecationDate > obj_deprecation_date
    ]

    if incorrect_deprecation_date:
        _print_error(
            "Deprecation Warning:",
            f"Downstream {entity_type} {incorrect_deprecation_date}"
            f" are missing deprecationDate or has a later deprecationDate than upstream {obj_name}."
            f" Please ensure either to remove/migrate the dependency or set up correct deprecationDate.",
        )


def check_deprecation_existence_in_upstream(collection, entity_type, obj_name):
    for name, item in collection.items():
        if item.metaData.deprecationDate:
            _print_error(
                "Deprecation Warning:",
                f"{entity_type} {name} is going to be deprecated by"
                f" {item.metaData.deprecationDate}. Please ensure either to remove/migrate"
                f" the dependency or set up correct deprecationDate for {obj_name}.",
            )


# For object with deprecation date set, check if downstream objects have correct deprecation date set.
# For object without deprecation date set, check if any upstream objects have deprecation date.
# Print out warning message if any of the above conditions are met.
def _handle_deprecation_warning(
    obj: Union[Join, GroupBy], obj_class: Type[Union[Join, GroupBy]], downstream_group_bys: dict, downstream_joins: dict
) -> None:
    if obj.metaData.deprecationDate:
        check_deprecation_date_setup_for_downstream(
            downstream_group_bys, "group_bys", obj.metaData.name, obj.metaData.deprecationDate
        )
        check_deprecation_date_setup_for_downstream(
            downstream_joins, "joins", obj.metaData.name, obj.metaData.deprecationDate
        )
    else:
        if obj_class is Join:
            check_deprecation_existence_in_upstream(
                {jp.groupBy.metaData.name: jp.groupBy for jp in obj.joinParts}, "Join part", obj.metaData.name
            )
        elif obj_class is GroupBy:
            check_deprecation_existence_in_upstream(
                {
                    source.joinSource.join.metaData.name: source.joinSource.join
                    for source in obj.sources
                    if source.joinSource
                },
                "Join source",
                obj.metaData.name,
            )


def _print_tables(obj: utils.ChrononJobTypes, obj_class: Type[utils.ChrononJobTypes]) -> None:
    tables = utils.get_modes_tables(obj)
    if obj_class is Join:
        _print_modes_tables("Output Join Tables", tables)
    if obj_class is GroupBy:
        _print_modes_tables("Output GroupBy Tables", tables)
    if obj_class is StagingQuery:
        _print_modes_tables("Output StagingQuery Tables", tables)


def _handle_extra_conf_objects_to_materialize(
    conf_objs,
    force_overwrite: bool,
    full_output_root: str,
    teams_path: str,
    validator: ChrononRepoValidator,
    log_level=logging.INFO,
    is_gb=True,
) -> None:
    num_written_objs = 0
    # load materialized joins to validate the additional conf objects against.
    validator.load_objs()
    for name, obj in conf_objs.items():
        team_name = name.split(".")[0]
        _set_team_level_metadata(obj, teams_path, team_name)
        if _write_obj(
            full_output_root,
            validator,
            name,
            obj,
            log_level,
            force_compile=True,
            force_overwrite=force_overwrite,
        ):
            num_written_objs += 1
    print(f"Successfully wrote {num_written_objs} {'GroupBy' if is_gb else 'Join'} objects to {full_output_root}")


def _set_team_level_metadata(obj: object, teams_path: str, team_name: str):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    table_properties = teams.get_team_conf(teams_path, team_name, "table_properties")
    obj.metaData.outputNamespace = obj.metaData.outputNamespace or namespace
    obj.metaData.tableProperties = obj.metaData.tableProperties or table_properties
    obj.metaData.team = team_name

    # set metadata for JoinSource
    if isinstance(obj, api.GroupBy):
        for source in obj.sources:
            if source.joinSource:
                _set_team_level_metadata(source.joinSource.join, teams_path, team_name)


def __fill_template(table, obj, namespace):
    if table:
        table = table.replace("{{ logged_table }}", utils.log_table_name(obj, full_name=True))
        table = table.replace("{{ db }}", namespace)
    return table


def _set_templated_values(obj, cls, teams_path, team_name):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    if cls == api.Join and obj.bootstrapParts:
        for bootstrap in obj.bootstrapParts:
            bootstrap.table = __fill_template(bootstrap.table, obj, namespace)
        if obj.metaData.dependencies:
            obj.metaData.dependencies = [__fill_template(dep, obj, namespace) for dep in obj.metaData.dependencies]
    if cls == api.Join and obj.labelPart:
        obj.labelPart.metaData.dependencies = [
            label_dep.replace("{{ join_backfill_table }}", utils.output_table_name(obj, full_name=True))
            for label_dep in obj.labelPart.metaData.dependencies
        ]


def _write_obj(
    full_output_root: str,
    validator: ChrononRepoValidator,
    name: str,
    obj: object,
    log_level: int,
    force_compile: bool = False,
    force_overwrite: bool = False,
) -> bool:
    """
    Returns True if the object is successfully written.
    """
    file_name, obj_class, output_file = _construct_output_file_name(full_output_root, name, obj)
    class_name = obj_class.__name__
    team_name = name.split(".")[0]
    _print_highlighted(f"{class_name} Team", team_name)
    _print_highlighted(f"{class_name} Name", file_name)
    skip_reasons = validator.can_skip_materialize(obj)
    if not force_compile and skip_reasons:
        reasons = ", ".join(skip_reasons)
        _print_warning(f"Skipping {class_name} {file_name}: {reasons}")
        if os.path.exists(output_file):
            _print_warning(f"old file exists for skipped config: {output_file}")
        return False
    validation_errors = validator.validate_obj(obj)
    if validation_errors:
        _print_error(f"Could not write {class_name} {file_name}", ", ".join(validation_errors))
        return False
    if force_overwrite:
        _print_warning(f"Force overwrite {class_name} {file_name}")
    elif not validator.safe_to_overwrite(obj):
        _print_warning(f"Cannot overwrite {class_name} {file_name} with existing online conf")
        return False
    _write_obj_as_json(file_name, obj, output_file, obj_class)
    return True


def _construct_output_file_name(full_output_root: str, name: str, obj: object) -> Tuple[str, Type, str]:
    team_name = name.split(".")[0]
    obj_class = type(obj)
    class_name = obj_class.__name__
    file_name = name.split(".", 1)[1]
    obj_folder_name = get_folder_name_from_class_name(class_name)
    output_path = os.path.join(full_output_root, obj_folder_name, team_name)
    output_file = os.path.join(output_path, file_name)
    return file_name, obj_class, output_file


def _write_obj_as_json(name: str, obj: object, output_file: str, obj_class: type):
    class_name = obj_class.__name__
    output_folder = os.path.dirname(output_file)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    assert os.path.isdir(output_folder), f"{output_folder} isn't a folder."
    assert hasattr(obj, "name") or hasattr(
        obj, "metaData"
    ), f"Can't serialize objects without the name attribute for object {name}"
    with open(output_file, "w") as f:
        _print_highlighted(f"Writing {class_name} to", output_file)
        f.write(thrift_simple_json_protected(obj, obj_class))


def _print_highlighted(left, right):
    # print in blue.
    print(f"{left:>25} - \u001b[34m{right}\u001b[0m")


def _print_features_names(left, right):
    # Print in green and separate lines.
    print(f"{left:>25} - \u001b[32m{right}\u001b[0m")


def _print_modes_tables(left, right):
    text = textwrap.indent(json.dumps(right, indent=2), " " * 27)
    json_start = "json.start"
    json_end = "json.end\n"

    print(f"{left:>25} - \n{json_start:>25} \u001b[32m\n{text}\u001b[0m \n{json_end:>25}")


def _print_error(left, right):
    # print in red.
    print(f"\033[91m{left:>25} - \033[1m{right}\033[00m")


def _print_warning(string):
    # print in yellow - \u001b[33m
    print(f"\u001b[33m{string}\u001b[0m")


def _print_debug_info(keys: List[str], header: str, log_level=logging.DEBUG):
    if log_level != logging.DEBUG:
        return

    print(f"{header}")
    right = pprint.pformat(list(keys)) if keys else "None"
    print(f"\t\u001b[34m{right}\u001b[0m")


if __name__ == "__main__":
    extract_and_convert()
