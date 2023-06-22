import ai.chronon.api.ttypes as api
import ai.chronon.repo.extract_objects as eo
import gc
import importlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterable
from typing import List, Union, cast, Optional


ChrononJobTypes = Union[api.GroupBy, api.Join, api.StagingQuery]


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
    table = source.entities.snapshotTable if source.entities else source.events.table
    return table.split('/')[0]


def get_topic(source: api.Source) -> str:
    return source.entities.mutationTopic if source.entities else source.events.topic


def get_columns(source: api.Source):
    query = get_query(source)
    assert query.selects is not None, "Please specify selects in your Source/Query"
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


def __set_name(obj, cls, mod_prefix):
    module = importlib.import_module(get_mod_name_from_gc(obj, mod_prefix))
    eo.import_module_set_name(module, cls)


def output_table_name(obj, full_name: bool):
    table_name = obj.metaData.name.replace('.', '_')
    db = obj.metaData.outputNamespace
    db = db or "{{ db }}"
    if full_name:
        return db + "." + table_name
    else:
        return table_name


def log_table_name(obj, full_name: bool = False):
    return output_table_name(obj, full_name=full_name) + "_logged"


def get_staging_query_output_table_name(staging_query: api.StagingQuery, full_name: bool = False):
    """generate output table name for staging query job"""
    __set_name(staging_query, api.StagingQuery, "staging_queries")
    return output_table_name(staging_query, full_name=full_name)


def get_join_output_table_name(join: api.Join, full_name: bool):
    """generate output table name for staging query job"""
    __set_name(join, api.Join, "joins")
    return output_table_name(join, full_name=full_name)


def get_dependencies(
        src: api.Source,
        dependencies: List[str] = None,
        meta_data: api.MetaData = None,
        lag: int = 0) -> List[str]:
    query = get_query(src)
    start = query.startPartition
    end = query.endPartition
    if meta_data is not None:
        result = [json.loads(dep) for dep in meta_data.dependencies]
    elif dependencies:
        result = [{
            "name": wait_for_name(dep),
            "spec": dep,
            "start": start,
            "end": end
        } for dep in dependencies]
    else:
        if src.entities and src.entities.mutationTable:
            # Opting to use no lag for all use cases because that the "safe catch-all" case when
            # it comes to dependencies (assuming ds lands before ds + 1). The actual query lag logic
            # is more complicated and depends on temporal/snapshot accuracy for join.
            result = list(filter(None, [
                wait_for_simple_schema(src.entities.snapshotTable, lag, start, end),
                wait_for_simple_schema(src.entities.mutationTable, lag, start, end)]))
        elif src.entities:
            result = [wait_for_simple_schema(src.entities.snapshotTable, lag, start, end)]
        else:
            result = [wait_for_simple_schema(src.events.table, lag, start, end)]
    return [json.dumps(res) for res in result]


def get_bootstrap_dependencies(bootstrap_parts) -> List[str]:
    if bootstrap_parts is None:
        return []

    dependencies = []
    for bootstrap_part in bootstrap_parts:
        table = bootstrap_part.table
        start = bootstrap_part.query.startPartition if bootstrap_part.query is not None else None
        end = bootstrap_part.query.endPartition if bootstrap_part.query is not None else None
        dependencies.append(
            wait_for_simple_schema(table, 0, start, end)
        )
    return [json.dumps(dep) for dep in dependencies]


def get_label_table_dependencies(label_part) -> List[str]:
    label_info = [(label.groupBy.sources, label.groupBy.metaData) for label in label_part.labels]
    label_info = [(source, meta_data) for (sources, meta_data) in label_info for source in sources]
    label_dependencies = [dep for (source, meta_data) in label_info for dep in
                          get_dependencies(src=source, meta_data=meta_data)]
    label_dependencies.append(
        json.dumps({
            "name": "wait_for_{{ join_backfill_table }}",
            "spec": "{{ join_backfill_table }}/ds={{ ds }}"
        })
    )
    return label_dependencies


def wait_for_simple_schema(table, lag, start, end):
    if not table:
        return None
    table_tokens = table.split('/')
    clean_name = table_tokens[0]
    subpartition_spec = '/'.join(table_tokens[1:]) if len(table_tokens) > 1 else ''
    return {
        "name": "wait_for_{}_ds{}".format(clean_name, "" if lag == 0 else f"_minus_{lag}"),
        "spec": "{}/ds={}{}".format(
            clean_name,
            "{{ ds }}" if lag == 0 else "{{{{ macros.ds_add(ds, -{}) }}}}".format(lag),
            '/{}'.format(subpartition_spec) if subpartition_spec else '',
        ),
        "start": start,
        "end": end,
    }


def wait_for_name(dep):
    replace_nonalphanumeric = re.sub('[^a-zA-Z0-9]', '_', dep)
    name = f"wait_for_{replace_nonalphanumeric}"
    return re.sub('_+', '_', name).rstrip('_')


def dedupe_in_order(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def has_topic(group_by: api.GroupBy) -> bool:
    """Find if there's topic or mutationTopic for a source helps define streaming tasks"""
    return any(
        (source.entities and source.entities.mutationTopic) or (source.events and source.events.topic)
        for source in group_by.sources
    )


def get_offline_schedule(conf: ChrononJobTypes) -> Optional[str]:
    schedule_interval = conf.metaData.offlineSchedule or "@daily"
    if schedule_interval == "@never":
        return None
    return schedule_interval


def requires_log_flattening_task(conf: ChrononJobTypes) -> bool:
    return (conf.metaData.samplePercent or 0) > 0


def get_applicable_modes(conf: ChrononJobTypes) -> List[str]:
    """Based on a conf and mode determine if a conf should define a task."""
    modes = []  # type: List[str]

    if isinstance(conf, api.GroupBy):
        group_by = cast(api.GroupBy, conf)
        if group_by.backfillStartDate is not None:
            modes.append("backfill")

        online = group_by.metaData.online or False

        if online:
            modes.append("upload")

            temporal_accuracy = group_by.accuracy or False
            streaming = has_topic(group_by)
            if temporal_accuracy or streaming:
                modes.append("streaming")
    elif isinstance(conf, api.Join):
        join = cast(api.Join, conf)
        if get_offline_schedule(conf) is not None:
            modes.append("backfill")
            modes.append("stats-summary")
        if (
            join.metaData.customJson is not None and
            json.loads(join.metaData.customJson).get("check_consistency") is True
        ):
            modes.append("consistency-metrics-compute")
        if requires_log_flattening_task(join):
            modes.append("log-flattener")
        if join.labelPart is not None:
            modes.append("label-join")
    elif isinstance(conf, api.StagingQuery):
        modes.append("backfill")
    else:
        raise ValueError(f"Unsupported job type {type(conf).__name__}")

    return modes


def get_related_table_names(conf: ChrononJobTypes) -> List[str]:
    table_name = output_table_name(conf, full_name=True)

    applicable_modes = set(get_applicable_modes(conf))
    related_tables = []  # type: List[str]

    if "upload" in applicable_modes:
        related_tables.append(f"{table_name}_upload")
    if "stats-summary" in applicable_modes:
        related_tables.append(f"{table_name}_daily_stats")
    if "label-join" in applicable_modes:
        related_tables.append(f"{table_name}_labels")
        related_tables.append(f"{table_name}_labeled")
        related_tables.append(f"{table_name}_labeled_latest")
    if "log-flattener" in applicable_modes:
        related_tables.append(f"{table_name}_logged")
    if "consistency-metrics-compute" in applicable_modes:
        related_tables.append(f"{table_name}_consistency")

    if isinstance(conf, api.Join) and conf.bootstrapParts:
        related_tables.append(f"{table_name}_bootstrap")

    return related_tables
