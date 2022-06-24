"""
Run the flow for materialize.
"""
from ai.chronon.repo.compile import extract_and_convert
from click.testing import CliRunner


def test_basic_compile():
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team/'
    ])
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team'
    ])
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team/sample_join.py'
    ])
    assert result.exit_code == 0


def test_debug_compile():
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team/',
        '--debug'
    ])
    assert result.exit_code == 0


def test_failed_compile():
    """
    Should fail as it fails to find teams.
    """
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--input_path=joins/sample_team/',
    ])
    assert result.exit_code != 0

def test_failed_compile_missing_input_column():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--zipline_root=test/sample',
        '--input_path=group_bys/sample_team/sample_group_by_missing_input_column.py',
        '--debug'
    ])
    assert result.exit_code != 0
