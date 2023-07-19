import pytest
import os


@pytest.fixture
def rootdir():
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def teams_json(rootdir):
    return os.path.join(rootdir, 'sample/teams.json')


@pytest.fixture
def repo(rootdir):
    return os.path.join(rootdir, 'sample/')


@pytest.fixture
def test_online_group_by(repo):
    return os.path.join(repo, 'production/group_bys/sample_team/event_sample_group_by.v1')


@pytest.fixture
def sleepless():
    def justpass(seconds):
        pass
    return justpass
