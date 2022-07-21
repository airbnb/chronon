import ai.chronon.api.ttypes as api
import ai.chronon.repo.extract_objects as eo
import ai.chronon.utils as utils
import copy
import gc
import importlib
import json
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)


def JoinPart(group_by: api.GroupBy,
             key_mapping: Dict[str, str] = None,  # mapping of key columns from the join
             prefix: str = None  # all aggregations will be prefixed with that name
             ) -> api.JoinPart:
    # used for reset for next run
    import_copy = __builtins__['__import__']
    # get group_by's module info from garbage collector
    gc.collect()
    group_by_module_name = None
    for ref in gc.get_referrers(group_by):
        if '__name__' in ref and ref['__name__'].startswith("group_bys"):
            group_by_module_name = ref['__name__']
            break
    logging.debug("group_by's module info from garbage collector {}".format(group_by_module_name))
    group_by_module = importlib.import_module(group_by_module_name)
    __builtins__['__import__'] = eo.import_module_set_name(group_by_module, api.GroupBy)
    if key_mapping:
        utils.check_contains(key_mapping.values(),
                             group_by.keyColumns,
                             "key",
                             group_by.metaData.name)

    join_part = api.JoinPart(
        groupBy=group_by,
        keyMapping=key_mapping,
        prefix=prefix
    )
    # reset before next run
    __builtins__['__import__'] = import_copy
    return join_part


def Join(left: api.Source,
         right_parts: List[api.JoinPart],
         check_consistency: bool = False,
         additional_args: List[str] = None,
         additional_env: List[str] = None,
         dependencies: List[str] = None,
         online: bool = False,
         production: bool = False,
         output_namespace: str = None,
         table_properties: Dict[str, str] = None,
         env: Dict[str, Dict[str, str]] = None,
         lag: int = 0,
         skew_keys: Dict[str, List[str]] = None,
         sample_percent: float = None,  # will sample all the requests based on sample percent
         **kwargs
         ) -> api.Join:
    # create a deep copy for case: multiple LeftOuterJoin use the same left,
    # validation will fail after the first iteration
    updated_left = copy.deepcopy(left)
    if left.events and left.events.query.selects:
        assert "ts" not in left.events.query.selects.keys(), "'ts' is a reserved key word for Chronon," \
                                                             " please specify the expression in timeColumn"
        # mapping ts to query.timeColumn to events only
        updated_left.events.query.selects.update({"ts": updated_left.events.query.timeColumn})
    # name is set externally, cannot be set here.
    # root_keys = set(root_base_source.query.select.keys())
    # for join_part in right_parts:
    #    mapping = joinPart.key_mapping if joinPart.key_mapping else {}
    #    # TODO: Add back validation? Or not?
    #    #utils.check_contains(mapping.keys(), root_keys, "root key", "")
    #    uncovered_keys = set(joinPart.groupBy.keyColumns) - set(mapping.values()) - root_keys
    #    assert not uncovered_keys, f"""
    #    Not all keys columns needed to join with GroupBy:{joinPart.groupBy.name} are present.
    #    Missing keys are: {uncovered_keys},
    #    Missing keys should be either mapped or selected in root.
    #    KeyMapping only mapped: {mapping.values()}
    #    Root only selected: {root_keys}
    #    """

    right_sources = [join_part.groupBy.sources for join_part in right_parts]
    # flattening
    right_sources = [source for source_list in right_sources for source in source_list]
    left_dependencies = utils.get_dependencies(left, dependencies, lag=lag)
    right_dependencies = [dep for source in right_sources for dep in
                          utils.get_dependencies(source, dependencies, lag=lag)]

    custom_json = {
        "check_consistency": check_consistency,
        "lag": lag
    }

    if additional_args:
        custom_json["additional_args"] = additional_args

    if additional_env:
        custom_json["additional_env"] = additional_env
    custom_json.update(kwargs)

    metadata = api.MetaData(
        online=online,
        production=production,
        customJson=json.dumps(custom_json),
        dependencies=utils.dedupe_in_order(left_dependencies + right_dependencies),
        outputNamespace=output_namespace,
        tableProperties=table_properties,
        modeToEnvMap=env,
        samplePercent=sample_percent
    )

    return api.Join(
        left=updated_left,
        joinParts=right_parts,
        metaData=metadata,
        skewKeys=skew_keys
    )
