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

import glob
import importlib.machinery
import importlib.util
import logging
import os

from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.logger import get_logger


def from_folder(root_path: str, full_path: str, cls: type, log_level=logging.INFO):
    """
    Recursively consumes a folder, and constructs a map
    Creates a map of object qualifier to
    """
    if full_path.endswith("/"):
        full_path = full_path[:-1]

    python_files = glob.glob(os.path.join(full_path, "**/*.py"), recursive=True)
    result = {}
    for f in python_files:
        try:
            result.update(from_file(root_path, f, cls, log_level))
        except Exception as e:
            logging.error(f"Failed to extract: {f}")
            logging.exception(e)
    return result


def import_module_set_name(module, cls, inline_group_bys=None):
    """evaluate imported modules to assign object name"""
    for name, obj in list(module.__dict__.items()):
        if isinstance(obj, cls):
            # the name would be `team_name.python_script_name.[group_by_name|join_name|staging_query_name]`
            # example module.__name__=group_bys.user.avg_session_length, name=v1
            # obj.metaData.name=user.avg_session_length.v1
            # obj.metaData.team=user
            obj.metaData.name = module.__name__.partition(".")[2] + "." + name
            obj.metaData.team = module.__name__.split(".")[1]
            if isinstance(obj, Join) and inline_group_bys:
                for jp in obj.joinParts:
                    if jp.groupBy in inline_group_bys:
                        jp.groupBy.metaData.team = obj.metaData.team

    return module


def from_file(root_path: str, file_path: str, cls: type, log_level=logging.INFO):
    logger = get_logger(log_level)
    logger.debug("Loading objects of type {cls} from {file_path}".format(**locals()))
    # mod_qualifier includes team name and python script name without `.py`
    # this line takes the full file path as input, strips the root path on the left side
    # strips `.py` on the right side and finally replaces the slash sign to dot
    # eg: the output would be `team_name.python_script_name`
    mod_qualifier = file_path[len(root_path.rstrip("/")) + 1 : -3].replace("/", ".")
    mod = importlib.import_module(mod_qualifier)

    # get inline group_bys
    inline_group_bys = [k for k in mod.__dict__.values() if isinstance(k, GroupBy)] if cls == Join else None

    # the key of result dict would be `team_name.python_script_name.[group_by_name|join_name|staging_query_name]`
    # real world case: psx.reservation_status.v1
    import_module_set_name(mod, cls, inline_group_bys)

    result = {}

    for obj in [o for o in mod.__dict__.values() if isinstance(o, cls)]:
        result[obj.metaData.name] = obj
    return result
