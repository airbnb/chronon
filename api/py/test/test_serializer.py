
from ai.chronon.repo.serializer import thrift_simple_json_protected
from ai.chronon.api.ttypes import Join

from src.python.shepherd.chronon_poc.joins.sample_team.sample_join import v1

import json

def test_serialize_join_with_zoolander_path_format():
    json_result = json.loads(thrift_simple_json_protected(v1, Join))
    assert json_result['joinParts'][0]['groupBy']['metaData']['name'] == 'sample_team.sample_group_by.v1'