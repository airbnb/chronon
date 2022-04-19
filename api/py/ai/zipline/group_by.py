import inspect
import json
from typing import List, Optional, Union, Dict, Callable, Tuple

import ai.zipline.api.ttypes as ttypes
import ai.zipline.utils as utils

OperationType = int  # type(zthrift.Operation.FIRST)

#  The GroupBy's default online/production status is None and it will inherit
# online/production status from the Joins it is included.
# If it is included in multiple joins, it is considered online/production
# if any of the joins are online/production. Otherwise it is not online/production
# unless it is explicitly marked as online/production on the GroupBy itself.
DEFAULT_ONLINE = None
DEFAULT_PRODUCTION = None


def collector(op: ttypes.Operation) -> Callable[[ttypes.Operation], Tuple[ttypes.Operation, Dict[str, str]]]:
    return lambda k: (op, {"k": str(k)})


# To simplify imports
class Accuracy(ttypes.Accuracy):
    pass


class Operation():
    MIN = ttypes.Operation.MIN
    MAX = ttypes.Operation.MAX
    FIRST = ttypes.Operation.FIRST
    LAST = ttypes.Operation.LAST
    APPROX_UNIQUE_COUNT = ttypes.Operation.APPROX_UNIQUE_COUNT
    UNIQUE_COUNT = ttypes.Operation.UNIQUE_COUNT
    COUNT = ttypes.Operation.COUNT
    SUM = ttypes.Operation.SUM
    AVERAGE = ttypes.Operation.AVERAGE
    VARIANCE = ttypes.Operation.VARIANCE
    # k truncates the map to top_k most frequent items, 0 turns off truncation
    HISTOGRAM_K = collector(ttypes.Operation.HISTOGRAM)
    FIRST_K = collector(ttypes.Operation.FIRST_K)
    LAST_K = collector(ttypes.Operation.LAST_K)
    TOP_K = collector(ttypes.Operation.TOP_K)
    BOTTOM_K = collector(ttypes.Operation.BOTTOM_K)


def Aggregations(**agg_dict):
    assert all(isinstance(agg, ttypes.Aggregation) for agg in agg_dict.values())
    for key, agg in agg_dict.items():
        if not agg.inputColumn and agg.operation != Operation.COUNT:
            agg.inputColumn = key
    return agg_dict.values()


def DefaultAggregation(keys, sources, operation=Operation.LAST):
    aggregate_columns = []
    for source in sources:
        query = utils.get_query(source)
        columns = utils.get_columns(source)
        non_aggregate_columns = keys + [
            "ts",
            "is_before",
            "mutation_ts",
            "ds",
            query.timeColumn
        ]
        aggregate_columns += [
            column
            for column in columns
            if column not in non_aggregate_columns
        ]
    return [
        Aggregation(
            operation=operation,
            input_column=column) for column in aggregate_columns
    ]


class TimeUnit():
    HOURS = ttypes.TimeUnit.HOURS
    DAYS = ttypes.TimeUnit.DAYS


def window_to_str_pretty(window: ttypes.Window):
    unit = ttypes.TimeUnit._VALUES_TO_NAMES[window.timeUnit].lower()
    return f"{window.length} {unit}"


def op_to_str(operation: OperationType):
    return ttypes.Operation._VALUES_TO_NAMES[operation].lower()


def Aggregation(input_column: str = None,
                operation: Union[ttypes.Operation, Tuple[ttypes.Operation, Dict[str, str]]] = None,
                windows: List[ttypes.Window] = None,
                buckets: List[str] = None) -> ttypes.Aggregation:
    # Default to last
    operation = operation if operation is not None else Operation.LAST
    arg_map = {}
    if isinstance(operation, tuple):
        operation, arg_map = operation[0], operation[1]
    return ttypes.Aggregation(input_column, operation, arg_map, windows, buckets)


def Window(length: int, timeUnit: ttypes.TimeUnit) -> ttypes.Window:
    return ttypes.Window(length, timeUnit)


def contains_windowed_aggregation(aggregations: Optional[List[ttypes.Aggregation]]):
    if not aggregations:
        return False
    for agg in aggregations:
        if agg.windows:
            return True
    return False


def validate_group_by(group_by: ttypes.GroupBy):
    sources = group_by.sources
    keys = group_by.keyColumns
    aggregations = group_by.aggregations
    # check ts is not included in query.select
    first_source_columns = set(utils.get_columns(sources[0]))
    assert "ts" not in first_source_columns, "'ts' is a reserved key word for Zipline," \
                                             " please specify the expression in timeColumn"
    for src in sources:
        query = utils.get_query(src)
        if src.events:
            assert query.mutationTimeColumn is None, "ingestionTimeColumn should not be specified for " \
                "event source as it should be the same with timeColumn"
            assert query.reversalColumn is None, "reversalColumn should not be specified for event source " \
                                                 "as it won't have mutations"
            if group_by.accuracy != Accuracy.SNAPSHOT:
                assert query.timeColumn is not None, "please specify query.timeColumn for non-snapshot accurate " \
                    "group by with event source"
        else:
            if contains_windowed_aggregation(aggregations):
                assert query.timeColumn, "Please specify timeColumn for entity source with windowed aggregations"

    # all sources should select the same columns
    for i, source in enumerate(sources[1:]):
        column_set = set(utils.get_columns(source))
        column_diff = column_set ^ first_source_columns
        assert not column_diff, f"""
Mismatched columns among sources [1, {i+2}], Difference: {column_diff}
"""

    # all keys should be present in the selected columns
    unselected_keys = set(keys) - first_source_columns
    assert not unselected_keys, f"""
Keys {unselected_keys}, are unselected in source
"""

    # Aggregations=None is only valid if group_by is Entities
    if aggregations is None:
        assert not any([s.events for s in sources]), "You can only set aggregations=None in an EntitySource"
    else:
        columns = set([c for src in sources for c in utils.get_columns(src)])
        for agg in aggregations:
            assert agg.inputColumn or agg.operation == Operation.COUNT, (
                f"input_column is required for all operations except COUNT, found: input_column = {agg.inputColumn} "
                f"and operation {op_to_str(agg.operation)}"
            )
            assert agg.inputColumn in columns or agg.inputColumn is None, (
                f"input_column for aggregation is not part of the query. Available columns: {column_set} "
                f"input_column: {agg.inputColumn}")


def GroupBy(sources: Union[List[ttypes.Source], ttypes.Source],
            keys: List[str],
            aggregations: Optional[List[ttypes.Aggregation]],
            online: bool = DEFAULT_ONLINE,
            production: bool = DEFAULT_PRODUCTION,
            backfill_start_date: str = None,
            dependencies: List[str] = None,
            env: Dict[str, Dict[str, str]] = None,
            table_properties: Dict[str, str] = None,
            output_namespace: str = None,
            accuracy: ttypes.Accuracy = None,
            lag: int = 0,
            **kwargs) -> ttypes.GroupBy:
    assert sources, "Sources are not specified"

    if isinstance(sources, ttypes.Source):
        sources = [sources]

    deps = [dep for src in sources for dep in utils.get_dependencies(src, dependencies, lag=lag)]

    kwargs.update({
        "lag": lag
    })
    # get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    metadata = ttypes.MetaData(
        online=online,
        production=production,
        outputNamespace=output_namespace,
        customJson=json.dumps(kwargs),
        dependencies=deps,
        modeToEnvMap=env,
        tableProperties=table_properties,
        team=team)

    group_by = ttypes.GroupBy(
        sources=sources,
        keyColumns=keys,
        aggregations=aggregations,
        metaData=metadata,
        backfillStartDate=backfill_start_date,
        accuracy=accuracy
    )
    validate_group_by(group_by)
    return group_by
