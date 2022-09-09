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


def write_test_config(team, obj, args, file, modulo = 1024, namespace='tmp'):
    """
    Write a temporary config for testing
    """
    obj.metaData.name = file.name.split('/')[-1]
    obj.metaData.outputNamespace = namespace
    mutated = mutate_obj(obj, modulo)
    _write_obj(
        'testing',
        validator=ChrononRepoValidator(
            chronon_root_path=args.repo,
            output_root='testing'),
        name=obj.metaData.name,
        obj=mutated,
        log_level=None,
        force_compile=False,
        force_overwrite=True
    )
    with open(file.name, 'r') as infile:
        print(infile.read())


def run_test_config(team, obj, days = 10, modulo = 1024, namespace='tmp', **kwargs):
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
    args = parse_args(base_args if base_args else None)
    with tempfile.NamedTemporaryFile(dir=os.path.join(args.repo, 'testing/joins', team)) as tmp:
        write_test_config(team, obj, args, tmp, modulo=modulo, suffix=suffix, namespace=namespace)
        args.conf = tmp
        build_runner(args).run()
