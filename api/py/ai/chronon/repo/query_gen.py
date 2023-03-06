#import ai.chronon.api.ttypes as ttypes
from ai.chronon.api.ttypes import GroupBy, Join
import ai.chronon.utils as utils
import logging
import inspect
import json
import click, os
from typing import List, Optional, Union, Dict, Callable, Tuple
from compile import FOLDER_NAME_TO_CLASS
#import extract_objects as eo
from datetime import datetime, timedelta

REMINDER_MESSAGE = "TODO"

# TODO: Allow format to be configurable!
YESTERDAY = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')


def get_primary_keys(obj: Union[ttypes.GroupBy, ttypes.Join]) -> List[str]:
    if isinstance(obj, ttypes.GroupBy):
        return obj.keyColumns
    else:
        return list(set([join_part.keyMapping.keys() for join_part in obj.joinParts]))


def gen_sampling_clause(sampling: Union[float, List[str], Dict[str, List[str]]]):
    return ""


def render_join_query(join: ttypes.Join, date: str, sample_rate: float, date_col: str):
    return ""


def render_group_by_query(group_by: ttypes.GroupBy, date: str, sample_rate: float, date_col: str):
    return ""


def run_sql_gen(results, ds, sampling):
    for name, obj in results.items():
        logging.info(f"Generating query for {name}: \n\n")
    print("HERE!")


@click.command()
@click.option(
    '--chronon_root',
    envvar='CHRONON_ROOT',
    help='Path to the root chronon folder',
    default=os.getcwd())
@click.option(
    '--input_path', '--conf',
    help='Relative Path to the root chronon folder, which contains the objects to be serialized',
    required=True)
@click.option(
    '--date', '--ds', '--day',
    help='Date for which you want to generate the query',
    default=YESTERDAY)
@click.option(
    '--sampling',
    help='Date for which you want to generate the query',
    default=0.01)
@click.option(
    '--date_col',
    help='The name of the date column in your source table(s), i.e. ds/day/date/etc.',
    default="ds")
def query_gen(chronon_root: str,
              input_path: str,
              date: str,
              sampling: Union[float, List[str], Dict[str, List[str]]],
              date_col: str = "ds"):
    """
    ## Extract the Python Object(s)
    chronon_root_path = os.path.expanduser(chronon_root)
    path_split = input_path.split('/')
    obj_folder_name = path_split[0]
    obj_class = FOLDER_NAME_TO_CLASS[obj_folder_name]
    assert (obj_class in [GroupBy, Join], f"{obj_class} are not yet support in query_gen.")
    full_input_path = os.path.join(chronon_root_path, input_path)
    assert os.path.exists(full_input_path), f"Input Path: {full_input_path} doesn't exist"
    assert full_input_path.endswith(".py"), f"Input Path: {input_path} isn't a python file"
    results = eo.from_file(chronon_root_path, full_input_path, obj_class)
    logging.info(f"Found {len(results)} {obj_class.__name__} objects: {list(results.keys())}")

    for name, obj in results.items():
        primary_keys = get_primary_keys(obj)
        if isinstance(sampling, float):
            assert (0 < sampling <= 1,
                    "If sampling by a rate, it must be a number between 0 and 1. You can use 1 if you do not want to downsample at all. Alternatively, you may pass a specific set of keys to sample down to.")
        elif isinstance(sampling, List):
            assert (len(primary_keys) == 1,
                    f"If sampling by a set of keys, you can only use a list of keys when there is a single primary key, in this case you must provide of key: value_set for each key in: {primary_keys}")
        else:
            all(key in primary_keys for key in sampling.keys())
            assert (all(key in primary_keys for key in sampling.keys()),
                    f"If sampling by a set of keys, you need to provide a key set for each unique key in your Join/GroupBy. In this case you have provided value sets for {sampling.keys()}, but you need to provide it for every key in: {primary_keys}")

        logging.info(f"Generating query for {name}: \n\n")
        if obj_class == GroupBy:
            query = render_group_by_query(obj, date, sampling, date_col)
        else:
            query = render_join_query(obj, date, sampling, date_col)
        logging.info(query)
        logging.info(REMINDER_MESSAGE)
    """


#if __name__ == '__main__':
#    query_gen()
