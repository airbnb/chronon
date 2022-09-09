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


def test_download_only_once(fake_process, teams_json):
    """
    Check the download only once util with fake subprocesses.
    Using teams_json just to have a valid path
    """
    # Does not download anything since all looks good.
    fake_process.register(["curl", "-sI", "http://fakeurl.com"], stdout="content-length: 103")
    fake_process.register(["wc", "-c", teams_json], stdout="103")
    run.download_only_once("http://fakeurl.com", teams_json)
    # Attempts to download. Raises exception on process not registered.
    fake_process.register(["curl", "-sI", "http://fakeurl.com"], stdout="content-length: 103")
    fake_process.register(["wc", "-c", teams_json], stdout="104")
    with pytest.raises(Exception):
        run.download_only_once("http://fakeurl.com", teams_json)

    # Attempts to download since there's no file.
    fake_process.register(
        ["curl", "http://fakeurl.com", "-o", "fakepath", "--connect-timeout", "10"],
        stdout="content-length: 103"
    )
    run.download_only_once("http://fakeurl.com", "fakepath")


def fake_xml():
    return """
<metadata>
    <groupId>ai.chronon</groupId>
    <artifactId>spark_uber_2.11</artifactId>
    <versioning>
        <latest>0.0.7</latest>
        <release>0.0.7</release>
        <versions>
            <version>local</version>
            <version>0.0.1</version>
        </versions>
        <lastUpdated>20220823025227</lastUpdated>
    </versioning>
</metadata>"""


def test_build_runner(fake_process):
    args = run.parse_args(None)
    # Will try to check versions and download jars.
    fake_process.register(['curl', '-s', f"{run.base_url('uber')}/maven-metadata.xml"], stdout=fake_xml())
    fake_process.register([
        'curl',
        f"{run.base_url('uber')}/0.0.1/spark_uber_2.11-0.0.1-assembly.jar",
        '-o', '/tmp/spark_uber_2.11-0.0.1-assembly.jar', '--connect-timeout', '10'], stdout="gibberish")
    run.build_runner(args)


@pytest.mark.parametrize("conf", ["production/joins/sample_team/sample_join.v1"])
def test_default_run(fake_process, conf):
    args = run.parse_args(
        ["--conf", conf, "--mode", "backfill", '--spark-submit-path', 'spark_submit.sh', '--ds', '2022-04-09'])
    # Will try to check versions and download jars.
    fake_process.register(['curl', '-s', f"{run.base_url('uber')}/maven-metadata.xml"], stdout=fake_xml())
    fake_process.register([
        'curl',
        f"{run.base_url('uber')}/0.0.1/spark_uber_2.11-0.0.1-assembly.jar",
        '-o', '/tmp/spark_uber_2.11-0.0.1-assembly.jar', '--connect-timeout', '10'], stdout="gibberish")

    # Fake process of spark submit.
    fake_process.register([
        'bash', 'spark_submit.sh', '--class', 'ai.chronon.spark.Driver', '/tmp/spark_uber_2.11-0.0.1-assembly.jar',
        'join', f"--conf-path={conf}", '--end-date=2022-04-09'])
    run.build_runner(args).run()
