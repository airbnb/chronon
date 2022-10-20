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
             key_mapping: Dict[str, str] = None,
             prefix: str = None) -> api.JoinPart:
    """
    Specifies HOW to join the `left` of a Join with GroupBy's.

    :param group_by:
        The GroupBy object to join with. Keys on left are used to equi join with keys on right.
        When left is entities all GroupBy's are computed as of midnight.
        When left is events, we do a point-in-time join when right.accuracy == TEMPORAL OR right.source.topic != null
    :type group_by: ai.chronon.api.GroupBy
    :param key_mapping:
        Names of keys don't always match on left and right, this mapping tells us how to map when they don't.
    :type key_mapping: Dict[str, str]
    :param prefix:
        All the output columns of the groupBy will be prefixed with this string. This is used when you need to join
        the same groupBy more than once with `left`. Say on the left you have seller and buyer, on the group you have
        a user's avg_price, and you want to join the left (seller, buyer) with (seller_avg_price, buyer_avg_price) you
        would use key_mapping and prefix parameters.
    :return:
        JoinPart specifies how the left side of a join, or the query in online setting, would join with the right side
        components like GroupBys.
    """
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
    """
    Construct a join object. A join can pull together data from various GroupBy's both offline and online. This is also
    the focal point for logging, data quality computation and monitoring. A join maps 1:1 to models in ML usage.

    :param left:
        The source on the left side, when Entities, all GroupBys are join with SNAPSHOT accuracy (midnight values).
        When left is events, if on the right, either when GroupBy's are TEMPORAL, or when topic is specified, we perform
        a TEMPORAL / point-in-time join.
    :type left: ai.chronon.api.Source
    :param right_parts:
        The list of groupBy's to join with. GroupBy's are wrapped in a JoinPart, which contains additional information
        on how to join the left side with the GroupBy.
    :type right_parts: List[ai.chronon.api.JoinPart]
    :param check_consistency:
        If online serving data should be compared with backfill data - as online-offline-consistency metrics.
        The metrics go into hive and your configured kv store for further visualization and monitoring.
    :type check_consistency: bool
    :param additional_args:
        Additional args go into `customJson` of `ai.chronon.api.MetaData` within the `ai.chronon.api.Join` object.
        This is a place for arbitrary information you want to tag your conf with.
    :type additional_args: List[str]
    :param additional_env:
        Deprecated, see env
    :type additional_env: List[str]
    :param dependencies:
        This goes into MetaData.dependencies - which is a list of string representing which table partitions to wait for
        Typically used by engines like airflow to create partition sensors.
    :type dependencies: List[str]
    :param online:
        Should we upload this conf into kv store so that we can fetch/serve this join online.
        Once Online is set to True, you ideally should not change the conf.
    :type online: bool
    :param production:
        This when set can be integrated to trigger alerts. You will have to integrate this flag into your alerting
        system yourself.
    :type production: bool
    :param output_namespace:
        In backfill mode, we will produce data into hive. This represents the hive namespace that the data will be
        written into. You can set this at the teams.json leve.
    :type output_namespace: str
    :param table_properties:
        Specifies the properties on output hive tables. Can be specified in teams.json.
    :param env:
        This is a dictionary of "mode name" to dictionary of "env var name" to "env var value".
        These vars are set in run.py and the underlying spark_submit.sh.
        There override vars set in teams.json/production/<MODE NAME>
        The priority order (descending) is:
            var set while using run.py "VAR=VAL run.py --mode=backfill <name>"
            var set here in Join's env param
            var set in team.json/<team>/<production>/<MODE NAME>
            var set in team.json/default/<production>/<MODE NAME>
    :type env: Dict[str, Dict[str, str]]
    :param lag:
        Param that goes into customJson. You can pull this out of the json at path "metaData.customJson.lag"
        This is used by airflow integration to pick an older hive partition to wait on.
    :param skew_keys:
        While back-filling, if there are known irrelevant keys - like user_id = 0 / NULL etc. You can specify them here.
        This is used to blacklist crawlers etc
    :param sample_percent:
        Online only parameter. What percent of online serving requests to this join should be logged into warehouse.
    :return:
        A join object that can be used to backfill or serve data. For ML use-cases this should map 1:1 to model.
    """
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
