#!/usr/bin/env python
# tool to materialize feature_sources and feature_sets into thrift configurations
# that chronon jobs can consume

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
@click.option(
    '--chronon_root',
    envvar='CHRONON_ROOT',
    help='Path to the root chronon folder',
    default=os.getcwd())
@click.option(
    '--input_path', '--conf', 'input_path',
    help='Relative Path to the root chronon folder, which contains the objects to be serialized',
    required=True)
@click.option(
    '--output_root',
    help='Relative Path to the root chronon folder, to where the serialized output should be written',
    default="production")
@click.option(
    '--debug',
    help='debug mode',
    is_flag=True)
@click.option(
    '--force-overwrite',
    help='Force overwriting existing materialized conf.',
    is_flag=True)
def extract_and_convert(chronon_root, input_path, output_root, debug, force_overwrite):
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
    for name, obj in results.items():
        team_name = name.split(".")[0]
        _set_team_level_metadata(obj, teams_path, team_name)
        _set_templated_values(obj, obj_class, teams_path, team_name)
        if _write_obj(full_output_root, validator, name, obj, log_level, force_overwrite, force_overwrite):
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


def _set_team_level_metadata(obj: object, teams_path: str, team_name: str):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    table_properties = teams.get_team_conf(teams_path, team_name, "table_properties")
    obj.metaData.outputNamespace = obj.metaData.outputNamespace or namespace
    obj.metaData.tableProperties = obj.metaData.tableProperties or table_properties
    obj.metaData.team = team_name


def __fill_template(table, obj, namespace):
    if table:
        table = table.replace('{{ logged_table }}', utils.log_table_name(obj, full_name=True))
        table = table.replace('{{ db }}', namespace)
    return table


def _set_templated_values(obj, cls, teams_path, team_name):
    namespace = teams.get_team_conf(teams_path, team_name, "namespace")
    if cls == api.Join and obj.bootstrapParts:
        for bootstrap in obj.bootstrapParts:
            bootstrap.table = __fill_template(bootstrap.table, obj, namespace)
        if obj.metaData.dependencies:
            obj.metaData.dependencies = [__fill_template(dep, obj, namespace) for dep in obj.metaData.dependencies]
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
    skip_reasons = validator.can_skip_materialize(obj)
    if not force_compile and skip_reasons:
        reasons = ', '.join(skip_reasons)
        _print_warning(f"Skipping {class_name} {name}: {reasons}")
        if os.path.exists(output_file):
            _print_warning(f"old file exists for skipped config: {output_file}")
        return False
    validation_errors = validator.validate_obj(obj)
    if validation_errors:
        _print_error(f"Could not write {class_name} {name}",
                     ', '.join(validation_errors))
        return False
    if force_overwrite:
        _print_warning(f"Force overwrite {class_name} {name}")
    elif not validator.safe_to_overwrite(obj):
        _print_warning(f"Cannot overwrite {class_name} {name} with existing online conf")
        return False
    _write_obj_as_json(name, obj, output_file, obj_class)
    return True


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
