import pytest
import os


@pytest.fixture
def rootdir():
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def teams_json(rootdir):
    return os.path.join(rootdir, 'sample/teams.json')


@pytest.fixture
def sleepless():
    def justpass(seconds):
        pass
    return justpass
