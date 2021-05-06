import ai.zipline.api.ttypes as api
from typing import List, Dict


# TODO: add docstrings to the arguments

def query(selects: Dict[str, str] = None,
          wheres: List[str] = None,
          startPartition: str = None,
          endPartition: str = None,
          timeColumn: str = None,
          setups: List[str] = [],
          mutationTimeColumn: str = None,
          reversalColumn: str = None) -> api.Query:

    return api.Query(selects, wheres, startPartition, endPartition, timeColumn, setups, mutationTimeColumn, reversalColumn)


def select(*args, **kwargs):
    args = {x: x for x in args}
    return {**args, **kwargs}
