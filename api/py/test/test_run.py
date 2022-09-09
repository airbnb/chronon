"""
Run the flow for materialize.
"""
from ai.chronon.repo import run

import pytest
import os


def test_args_parser():
    """
    Basic test on arg parser.
    """
    # Test default values
    args = run.parse_args(None)
    assert args.spark_submit_path == os.path.join(os.environ['CHRONON_REPO_PATH'], 'scripts/spark_submit.sh')
    # Test default override
    args = run.parse_args(['--spark-submit-path', 'custom_submit.sh'])
    assert args.spark_submit_path == 'custom_submit.sh'
    # Test that unknown args get passed as args.
    args = run.parse_args(['--some-other-arg', 'custom'])
    assert args.args == '--some-other-arg custom'
