import inspect
import re
import pytest

from ai.chronon.api import ttypes
from ai.chronon import source


def _camel_to_snake(s: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()


@pytest.mark.parametrize("binding,ttype", [
    (source.EventSource, ttypes.EventSource),
    (source.EntitySource, ttypes.EntitySource),
    (source.JoinSource, ttypes.JoinSource),
])
def test_binding_signature_matches_ttype(binding, ttype):
    ignore = ["self", "customJson"]

    ttypes_params = {_camel_to_snake(param)
                     for param in inspect.signature(ttype.__init__).parameters
                     if param not in ignore}
    binding_params = {param.name
                      for param in inspect.signature(binding).parameters.values()
                      if param.kind != param.VAR_KEYWORD # ignore kwargs
                     }

    assert ttypes_params == binding_params, \
        (f"Params of Thrift object {ttype.__name__} don't match Python binding params.\n"
         f"Missing: {ttypes_params - binding_params}\n"
         f"Not expected: {binding_params - ttypes_params}")
