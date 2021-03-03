import glob
import importlib.machinery
import importlib.util
import logging
import os

import ai.zipline.utils as utils
from ai.zipline.logger import get_logger


def from_folder(root_path: str,
                full_path: str,
                cls: type,
                log_level=logging.INFO):
    """
    Recursively consumes a folder, and constructs a map
    Creates a map of object qualifier to
    """
    if full_path.endswith('/'):
        full_path = full_path[:-1]

    python_files = glob.glob(
        os.path.join(full_path, "**/*.py"),
        recursive=True)
    result = {}
    for f in python_files:
        result.update(from_file(root_path, f, cls, log_level))
    return result


def from_file(root_path: str,
              file_path: str,
              cls: type,
              log_level=logging.INFO):
    logger = get_logger(log_level)
    logger.debug(
        "Loading objects of type {cls} from {file_path}".format(**locals()))
    # mod_qualifier includes team name and python script name without `.py`
    # this line takes the full file path as input, strips the root path on the left side
    # strips `.py` on the right side and finally replaces the slash sign to dot
    # eg: the output would be `team_name.python_script_name`
    mod_qualifier = file_path[len(root_path.rstrip('/')) + 1:-3].replace("/", ".")
    mod = importlib.import_module(mod_qualifier)

    # the key of result dict would be `team_name.python_script_name.[group_by_name|join_name|staging_query_name]`
    # real world case: psx.reservation_status.v1
    utils.import_module_set_name(mod, cls)
    result = {obj.name: obj
              for obj in mod.__dict__.values() if isinstance(obj, cls)}
    return result
