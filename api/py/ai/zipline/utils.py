import ai.zipline.api.ttypes as api
import ai.zipline.repo.extract_objects as eo
import gc
import importlib
import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterable
from typing import List, Union


def edit_distance(str1, str2):
    m = len(str1) + 1
    n = len(str2) + 1
    dp = [[0 for _ in range(n)] for _ in range(m)]
    for i in range(m):
        for j in range(n):
            if i == 0:
                dp[i][j] = j
            elif j == 0:
                dp[i][j] = i
            elif str1[i - 1] == str2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i][j - 1],
                                   dp[i - 1][j],
                                   dp[i - 1][j - 1])
    return dp[m - 1][n - 1]


class JsonDiffer:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.new_name = 'new.json'
        self.old_name = 'old.json'

    def diff(
            self,
            new_json_str: object,
            old_json_str: object,
            skipped_keys=[]) -> str:
        new_json = {k: v for k, v in json.loads(new_json_str).items()
                    if k not in skipped_keys}
        old_json = {k: v for k, v in json.loads(old_json_str).items()
                    if k not in skipped_keys}

        with open(os.path.join(self.temp_dir, self.old_name), mode='w') as old, \
                open(os.path.join(self.temp_dir, self.new_name), mode='w') as new:
            old.write(json.dumps(old_json, sort_keys=True, indent=2))
            new.write(json.dumps(new_json, sort_keys=True, indent=2))
        diff_str = subprocess.run(['diff', old.name, new.name], stdout=subprocess.PIPE).stdout.decode('utf-8')
        return diff_str

    def clean(self):
        shutil.rmtree(self.temp_dir)


def check_contains_single(candidate, valid_items, type_name, name, print_function=repr):
    name_suffix = f"for {name}" if name else ""
    candidate_str = print_function(candidate)
    if not valid_items:
        assert f"{candidate_str}, is not a valid {type_name} because no {type_name}s are specified {name_suffix}"
    elif candidate not in valid_items:
        sorted_items = sorted(map(print_function, valid_items),
                              key=lambda item: edit_distance(candidate_str, item))
        printed_items = '\n    '.join(sorted_items)
        assert candidate in valid_items, f"""{candidate_str}, is not a valid {type_name} {name_suffix}
Please pick one from:
    {printed_items}
"""


def check_contains(candidates, *args):
    if isinstance(candidates, Iterable) and not isinstance(candidates, str):
        for candidate in candidates:
            check_contains_single(candidate, *args)
    else:
        check_contains_single(candidates, *args)


def get_streaming_sources(group_by: api.GroupBy) -> List[api.Source]:
    """Checks if the group by has a source with streaming enabled."""
    return [source for source in group_by.sources if is_streaming(source)]


def is_streaming(source: api.Source) -> bool:
    """Checks if the source has streaming enabled."""
    return (source.entities and source.entities.mutationTopic is not None) or \
        (source.events and source.events.topic is not None)


def get_underlying_source(source: api.Source) -> Union[api.EventSource, api.EntitySource]:
    return source.entities if source.entities else source.events


def get_query(source: api.Source) -> api.Query:
    return get_underlying_source(source).query


def get_table(source: api.Source) -> str:
    return source.entities.snapshotTable if source.entities else source.events.table


def get_topic(source: api.Source) -> str:
    return source.entities.mutationTopic if source.entities else source.events.topic


def get_columns(source: api.Source):
    query = get_query(source)
    columns = query.selects.keys()
    return columns


def get_mod_name_from_gc(obj, mod_prefix):
    """get an object's module information from garbage collector"""
    mod_name = None
    # get obj's module info from garbage collector
    gc.collect()
    for ref in gc.get_referrers(obj):
        if '__name__' in ref and ref['__name__'].startswith(mod_prefix):
            mod_name = ref['__name__']
            break
    return mod_name


def get_staging_query_output_table_name(staging_query: api.StagingQuery):
    """generate output table name for staging query job"""
    staging_query_module = importlib.import_module(get_mod_name_from_gc(staging_query, "staging_queries"))
    eo.import_module_set_name(staging_query_module, api.StagingQuery)
    return staging_query.metaData.name.replace('.', '_')
