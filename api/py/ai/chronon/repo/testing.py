"""
Methods to simplify testing.

* Sampling keys to reduce input data.
* Change name for output table to avoid writing sample data in production.
"""
from ai.chronon.repo.validator import ChrononRepoValidator
from ai.chronon.repo.compile import _write_obj
from ai.chronon.utils import get_underlying_source, get_query
from ai.chronon.api import ttypes

from ai.chronon.repo.run import parse_args, build_runner

from datetime import datetime, timedelta

import tempfile
import hashlib
import sys
import os


def mutate_obj(obj, modulo):
    """
    Add additional where clauses for obj to reduce the input size.
    """
    if isinstance(obj, ttypes.Join):
        mutate_join(obj, modulo)
    elif isinstance(obj, ttypes.GroupBy):
        mutate_group_by(obj, modulo)
    return obj


def additional_wheres(keys, query, modulo):
    mapped_keys = map(lambda x: query.selects[x], keys) if query.selects is not None else keys
    return [f"hash({key}) % {modulo} = 0" for key in mapped_keys]


def mutate_group_by(obj, modulo):
    """
    Group by modifications.
    """
    keys = obj.keyColumns
    for source in map(lambda x: get_underlying_source(x), obj.sources):
        wheres = additional_wheres(keys, source.query, modulo)
        source.query.wheres = wheres if not source.query.wheres else list(set(source.query.wheres + wheres))
    return obj


def mutate_join(obj, modulo):
    """
    Mutate a join source to filter keys.
    """
    keys = []
    for jp in obj.joinParts:
        keys.extend(jp.groupBy.keyColumns)
    source = get_underlying_source(obj.left)
    wheres = additional_wheres(keys, source.query, modulo)
    source.query.wheres = wheres if not source.query.wheres else list(set(source.query.wheres + wheres))
    for jp in obj.joinParts:
        jp.groupBy = mutate_group_by(jp.groupBy, modulo)
    return obj


def test_filename(obj):
    return f"testing.{hashlib.md5(str(obj).encode('utf8')).hexdigest()}"


def write_test_config(team, obj, args, modulo = 1024, namespace='tmp'):
    """
    Write a temporary config for testing
    """
    name = test_filename(obj)
    obj.metaData.name = f"{team}.{name}"
    obj.metaData.outputNamespace = namespace
    mutated = mutate_obj(obj, modulo)
    _write_obj(
        os.path.join(args.repo, 'production'),
        validator=ChrononRepoValidator(
            chronon_root_path=args.repo,
            output_root='production'),
        name=obj.metaData.name,
        obj=mutated,
        log_level=None,
        force_compile=False,
        force_overwrite=True
    )
    return os.path.join(args.repo, "production", "joins", team, name)


def run_test_config(team, obj, modulo = 1024, namespace='tmp', **kwargs):
    """
    Main method for testing.
    Creates a temporary file for the object and runs the job.
    Requires:
        team: Team name.
        obj: Join object to test.
    Optional:
        days: number of days.
        modulo: sampling the higher the smaller the output.
        namespace: namespace for the test tables.
        suffix: suffix for the test config name.
        **kwargs: Any other arguments usually passed through CLI to run.py in the form of a list.
        """
    base_args = []
    for k, v in kwargs.items():
        base_args.extend([k, v])
    args = parse_args(base_args if base_args else sys.argv[1:])
    filename = write_test_config(team, obj, args, modulo=modulo, namespace=namespace)
    args.conf = filename
    try:
        build_runner(args).run()
    finally:
        os.remove(filename)
