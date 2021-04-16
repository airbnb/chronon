import ai.zipline.api.ttypes as api
from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Tuple, Dict, Callable
import inspect

MIN = api.Operation.MIN
MAX = api.Operation.MAX
FIRST = api.Operation.FIRST
LAST = api.Operation.LAST
APPROX_UNIQUE_COUNT = api.Operation.APPROX_UNIQUE_COUNT
UNIQUE_COUNT = api.Operation.UNIQUE_COUNT
COUNT = api.Operation.COUNT
SUM = api.Operation.SUM
AVERAGE = api.Operation.AVERAGE
VARIANCE = api.Operation.VARIANCE
SKEW = api.Operation.SKEW
KURTOSIS = api.Operation.KURTOSIS
APPROX_PERCENTILE = api.Operation.APPROX_PERCENTILE


def collector(op: api.Operation) -> Callable[[int], Tuple[api.Operation, Dict[str, str]]]:
    return lambda k: (op, {"k": str(k)})


LAST_K = collector(api.Operation.LAST_K)
FIRST_K = collector(api.Operation.FIRST_K)
TOP_K = collector(api.Operation.TOP_K)
BOTTOM_K = collector(api.Operation.BOTTOM_K)


def parse_window(window_str: str) -> api.Window:
    unit_str = window_str[-1]
    length = int(window_str[:-1])
    assert unit_str in ["d", "h"], f"units should be one of 'd' - for day or 'h' - for hours, but found {unit_str}"
    unit_value = api.TimeUnit.DAYS if unit_str == "d" else api.TimeUnit.HOURS
    return api.Window(length=length, timeUnit=unit_value)


OpWithArgs = Tuple[api.Operation, Dict[str, str]]
OpWithOptionalArgs = Union[api.Operation, OpWithArgs]


def _normalize_to_tuple(v):
    if type(v) is tuple:
        return v
    else:
        return (v, None)


@dataclass
class Agg:
    column: str
    expression: str = None
    op: OpWithOptionalArgs = None
    ops: List[OpWithOptionalArgs] = None
    window: str = None
    windows: List[str] = None

    def _all_ops(self) -> List[OpWithArgs]:
        ops = ([self.op] if self.op is not None else []) + (self.ops if self.ops is not None else [])
        return [_normalize_to_tuple(op) for op in ops]

    def _all_window_groups(self) -> List[api.Window]:
        return ([self.window] if self.window is not None else []) + (self.windows if self.windows is not None else [])

    def unpack(self) -> List[api.Aggregation]:
        assert ~((self.ops is None) ^ (self.op is None)), "should set exactly one of `op` or `ops` of an Agg"
        assert (self.window is None) or (self.windows is None), "should not set both window and windows in Agg"
        aggregations = []
        for op in self._all_ops():
            windows = self._all_window_groups()
            if not windows or None in windows:
                agg = api.Aggregation(inputColumn=self.column, operation=op[0], argMap=op[1], windows=None)
                aggregations.append(agg)
            pure_windows = [parse_window(window) for window in windows if window is not None]
            if len(pure_windows) > 0:
                agg = api.Aggregation(inputColumn=self.column, operation=op[0], argMap=op[1], windows=pure_windows)
                aggregations.append(agg)
        return aggregations


class DataModel(Enum):
    ENTITIES = 1
    EVENTS = 2


# ditch the name and generate json using compiler.py
# single source aggregations are the most common, and can be easier to express
def GroupBy(name: str,
            table: str,
            model: DataModel,
            keys: List[Union[str, Tuple[str, str]]],
            aggs: List[Agg],  # data needs aggregation
            start_partition: str,
            outputNamespace: str = None,
            tableProperties: Dict[str, str] = None,
            selects: Dict[str, str] = None,  # data is pre-aggregated
            ts: str = None,
            wheres: List[str] = None,
            production: bool = False,
            online: bool = False,
            topic: str = None,
            mutation_table: str = None) -> api.GroupBy:
    assert (aggs is None) ^ (selects is None), "specity only one of aggs or selects"
    final_selects = selects
    aggregations = None
    if aggs is not None:
        final_selects = {agg.column: agg.expression if agg.expression else agg.column for agg in aggs}
        aggregations = [col_agg for agg in aggs for col_agg in agg.unpack()]

    key_selects = {tup[0]: tup[1] for tup in map(_normalize_to_tuple, keys)}
    common_cols = set(final_selects.keys()).intersection(key_selects.keys())
    assert len(common_cols) == 0, "columns in agg cannot overlap with columns in keys"
    final_selects.update(key_selects)
    query = api.Query(
        selects=final_selects,
        wheres=wheres,
        startPartition=start_partition,
        timeColumn=ts)

    source = None
    if model is DataModel.ENTITIES:
        source = api.Source(entities=api.EntitySource(snapshotTable=table,
                                                      mutationTopic=topic,
                                                      mutationTable=mutation_table,
                                                      query=query))

    if model is DataModel.EVENTS:
        source = api.Source(events=api.EventSource(table=table,
                                                   topic=topic,
                                                   query=query))
    # get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    return api.GroupBy(
        metaData=api.MetaData(name=name,
                              production=production,
                              online=online,
                              tableProperties=tableProperties,
                              outputNamespace=outputNamespace,
                              team=team),
        sources=[source],
        keyColumns=key_selects.keys(),
        aggregations=aggregations
    )
