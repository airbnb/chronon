import ai.zipline.api.ttypes as api
from typing import List, Dict


# TODO: add docstrings to the arguments

def Query(selects: Dict[str, str] = None,
          wheres: List[str] = None,
          start_partition: str = None,
          end_partition: str = None,
          time_column: str = None,
          setups: List[str] = [],
          mutation_time_column: str = None,
          reversal_column: str = None) -> api.Query:

    return api.Query(
        selects,
        wheres,
        start_partition,
        end_partition,
        time_column,
        setups,
        mutation_time_column,
        reversal_column)


def select(*args, **kwargs):
    args = {x: x for x in args}
    return {**args, **kwargs}
