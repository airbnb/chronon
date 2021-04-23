"""
Run the flow for materialize.
"""
from ai.zipline.repo.compile import extract_and_convert
from click.testing import CliRunner


def test_basic_compile():
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--zipline_root=test/sample',
        '--input_path=joins/sample_team/'
    ])
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, [
        '--zipline_root=test/sample',
        '--input_path=joins/sample_team'
    ])
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, [
        '--zipline_root=test/sample',
        '--input_path=joins/sample_team/sample_join.py'
    ])
    assert result.exit_code == 0


def test_debug_compile():
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--zipline_root=test/sample',
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
