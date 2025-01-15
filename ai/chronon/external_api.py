from typing import List

import ai.chronon.api.ttypes as api
import ai.chronon.repo.validator as validator


def get_join_output_columns(join: api.Join) -> List[str]:
    """
    From the join object, get the final output columns after derivations.
    This excludes the source keys.
    """
    return validator.get_join_output_columns(join).get(validator.FeatureDisplayKeys.OUTPUT_COLUMNS, [])
