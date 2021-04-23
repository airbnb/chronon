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
