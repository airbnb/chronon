#!/usr/bin/env python
# tool to materialize feature_sources and feature_sets into thrift configurations
# that chronon jobs can consume

import argparse
import logging
import os

import click

import ai.chronon.api.ttypes as api
import ai.chronon.repo.extract_objects as eo
import ai.chronon.utils as utils
from ai.chronon.api.ttypes import GroupBy, Join, StagingQuery
from ai.chronon.repo import JOIN_FOLDER_NAME, \
    GROUP_BY_FOLDER_NAME, STAGING_QUERY_FOLDER_NAME, TEAMS_FILE_PATH
from ai.chronon.repo import teams
from ai.chronon.repo.serializer import thrift_simple_json_protected
from ai.chronon.repo.validator import ChrononRepoValidator
from sql_gen import run_sql_gen
from datetime import datetime, timedelta
from run import APP_NAME_TEMPLATE, set_common_env
import run


# TODO: Allow format to be configurable!
YESTERDAY = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

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


def extract_and_convert():
    """
    CLI tool to convert Python chronon GroupBy's, Joins and Staging queries into their thrift representation.
    The materialized objects are what will be submitted to spark jobs - driven by airflow, or by manual user testing.
    """
    parser = argparse.ArgumentParser(description='Submit various kinds of chronon jobs')
    chronon_repo_path = os.getenv('CHRONON_REPO_PATH', os.getcwd())
    set_common_env(chronon_repo_path)
    parser.add_argument('--conf', required=False,
                        help='Conf param - required for every mode except fetch')
    parser.add_argument('--ds', default=YESTERDAY)
    parser.add_argument('--app-name', help='app name. Default to {}'.format(APP_NAME_TEMPLATE),
                        default=None)
    parser.add_argument('--repo', help='Path to chronon repo', default=chronon_repo_path)
    parser.add_argument('--version', help='Chronon version to use.',
                        default=os.environ.get('VERSION', None)),
    parser.add_argument('--sub-help', action='store_true',
                        help='print help command of the underlying jar and exit')
    parser.add_argument('--conf-type', default='group_bys',
                        help='related to sub-help - no need to set unless you are not working with a conf')
    parser.add_argument('--output_root', default='production',
                        help='Relative Path to the root chronon folder, to where the serialized output should be written')
    parser.add_argument('--online-args', default=os.getenv('CHRONON_ONLINE_ARGS', ''),
                        help='Basic arguments that need to be supplied to all online modes')
    parser.add_argument('--chronon-jar', default=None, help='Path to chronon OS jar')
    parser.add_argument('--release-tag', default=None,
                        help='Use the latest jar for a particular tag.')
    parser.add_argument('--sql', action='store_true',
                        help='Generates a SQL Query for testing/debugging.')
    parser.add_argument('--debug', action='store_true',
                        help='Debug mode.')
    parser.add_argument('--force_overwrite', action='store_true',
                        help='Force overwrite the config file.')
    parser.add_argument('--sampling', default=0.01, help='Sampling rate to use in rendering SQL query')

    args, unknown_args = parser.parse_known_args()

    if args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    _print_highlighted("Using chronon root path", args.repo)
    chronon_root_path = os.path.expanduser(args.repo)
    input_path = args.conf
    output_root = args.output_root
    force_overwrite = args.force_overwrite
    path_split = input_path.split('/')
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
    extra_online_group_bys = {}
    num_written_objs = 0
    full_output_root = os.path.join(chronon_root_path, output_root)
    teams_path = os.path.join(chronon_root_path, TEAMS_FILE_PATH)
    output_files = []
    for name, obj in results.items():
        team_name = name.split(".")[0]
        _set_team_level_metadata(obj, teams_path, team_name)
        _set_templated_values(obj, obj_class, teams_path, team_name)
        output_file = _write_obj(full_output_root, validator, name, obj, log_level, force_overwrite, force_overwrite)
        if output_file:
            output_files.append(os.path.join(output_root, output_file))
            num_written_objs += 1

            # In case of online join, we need to materialize the underlying online group_bys.
            if obj_class is Join and obj.metaData.online:
                online_group_bys = {}
                offline_gbs = []  # gather list to report errors
                for jp in obj.joinParts:
                    if jp.groupBy.metaData.online:
                        online_group_bys[jp.groupBy.metaData.name] = jp.groupBy
                    else:
                        offline_gbs.append(jp.groupBy.metaData.name)
                extra_online_group_bys.update(online_group_bys)
                assert not offline_gbs, \
                    "You must make all dependent GroupBys `online` if you want to make your join `online`." \
                    " You can do this by passing the `online=True` argument to the GroupBy constructor." \
                    " Fix the following: {}".format(offline_gbs)
    if extra_online_group_bys:
        num_written_group_bys = 0
        # load materialized joins to validate the additional group_bys against.
        validator.load_objs()
        for name, obj in extra_online_group_bys.items():
            team_name = name.split(".")[0]
            _set_team_level_metadata(obj, teams_path, team_name)
            if _write_obj(full_output_root, validator, name, obj, log_level,
                          force_compile=True, force_overwrite=force_overwrite):
                num_written_group_bys += 1
        print(f"Successfully wrote {num_written_group_bys} online GroupBy objects to {full_output_root}")
    if num_written_objs > 0:
        print(f"Successfully wrote {num_written_objs} {(obj_class).__name__} objects to {full_output_root}")

    if args.sql:
        args.mode = "sql"
        args.spark_version = '2.4.0'  # Unused but required to be set for run.py
        args.online_jar = None  # Unused but required to be set for run.py
        args.online_class = None
        args.spark_submit_path = None
        args.list_apps = None
        for conf in output_files:
            args.conf = conf
            run.Runner(args, unknown_args).run()


def _set_team_level_metadata(obj: object, teams_path: str, team_name: str):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    table_properties = teams.get_team_conf(teams_path, team_name, "table_properties")
    obj.metaData.outputNamespace = obj.metaData.outputNamespace or namespace
    obj.metaData.tableProperties = obj.metaData.tableProperties or table_properties
    obj.metaData.team = team_name


def _set_templated_values(obj, cls, teams_path, team_name):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    if cls == api.Join and obj.bootstrapParts:
        for bootstrap in obj.bootstrapParts:
            table = bootstrap.table
            if table:
                table = table.replace('{{ logged_table }}', utils.log_table_name(obj, full_name=True))
                table = table.replace('{{ db }}', namespace)
            bootstrap.table = table
    if cls == api.Join and obj.labelPart:
        obj.labelPart.metaData.dependencies = [label_dep.replace('{{ join_backfill_table }}',
                                                                 utils.output_table_name(obj, full_name=True))
                                               for label_dep in obj.labelPart.metaData.dependencies]


def _write_obj(full_output_root: str,
               validator: ChrononRepoValidator,
               name: str,
               obj: object,
               log_level: int,
               force_compile: bool = False,
               force_overwrite: bool = False) -> bool:
    """
    Returns True if the object is successfully written.
    """
    team_name = name.split(".")[0]
    obj_class = type(obj)
    class_name = obj_class.__name__
    name = name.split('.', 1)[1]
    _print_highlighted(f"{class_name} Team", team_name)
    _print_highlighted(f"{class_name} Name", name)
    obj_folder_name = get_folder_name_from_class_name(class_name)
    output_path = os.path.join(full_output_root, obj_folder_name, team_name)
    output_file = os.path.join(output_path, name)
    relative_path = os.path.join(obj_folder_name, team_name, name)
    skip_reasons = validator.can_skip_materialize(obj)
    if not force_compile and skip_reasons:
        reasons = ', '.join(skip_reasons)
        _print_warning(f"Skipping {class_name} {name}: {reasons}")
        if os.path.exists(output_file):
            _print_warning(f"old file exists for skipped config: {output_file}")
        return
    validation_errors = validator.validate_obj(obj)
    if validation_errors:
        _print_error(f"Could not write {class_name} {name}",
                     ', '.join(validation_errors))
        return
    if force_overwrite:
        _print_warning(f"Force overwrite {class_name} {name}")
    elif not validator.safe_to_overwrite(obj):
        _print_warning(f"Cannot overwrite {class_name} {name} with existing online conf")
        return
    _write_obj_as_json(name, obj, output_file, obj_class)
    return relative_path


def _write_obj_as_json(name: str, obj: object, output_file: str, obj_class: type):
    class_name = obj_class.__name__
    output_folder = os.path.dirname(output_file)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    assert os.path.isdir(output_folder), f"{output_folder} isn't a folder."
    assert (hasattr(obj, "name") or hasattr(obj, "metaData")), \
        f"Can't serialize objects without the name attribute for object {name}"
    with open(output_file, "w") as f:
        _print_highlighted(f"Writing {class_name} to", output_file)
        f.write(thrift_simple_json_protected(obj, obj_class))


def _print_highlighted(left, right):
    # print in blue.
    print(f"{left:>25} - \u001b[34m{right}\u001b[0m")


def _print_error(left, right):
    # print in red.
    print(f"\033[91m{left:>25} - \033[1m{right}\033[00m")


def _print_warning(string):
    # print in yellow - \u001b[33m
    print(f"\u001b[33m{string}\u001b[0m")


if __name__ == '__main__':
    extract_and_convert()
