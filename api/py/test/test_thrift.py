"""
As our thrift object evolve we will have some assumptions that change.
It'd be good to have test to ensure we don't merge an update breaking our assumptions.
"""
from ai.zipline.api import ttypes, constants
from ai.zipline.repo import serializer
import os


def test_thrift():
    """
    Ensure some fields are included in our thrift configs.
    """
    metadata = ttypes.MetaData()
    assert hasattr(metadata, 'team')


def test_read_production_join(rootdir):
    """
    Ensure we can read the join properly.
    """

    join = serializer.file2thrift(
        os.path.join(rootdir, 'sample/production/joins/sample_team/sample_join.v1'),
        ttypes.Join
    )
    assert join.metaData.team == 'sample_team'
