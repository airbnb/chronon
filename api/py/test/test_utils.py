from ai.zipline import utils
import pytest


@pytest.fixture
def event_group_by():
    """
    Sample source taken from one of the group bys.
    This is an event source, not streaming
    """
    from sample.group_bys.sample_team.sample_group_by_from_module import v1
    return v1


@pytest.fixture
def event_source(event_group_by):
    """
    Sample source taken from one of the group bys.
    This is an event source, not streaming
    """
    return event_group_by.sources[0]


def test_edit_distance():
    assert utils.edit_distance('test', 'test') == 0
    assert utils.edit_distance('test', 'testy') > 0
    assert utils.edit_distance("test", "testing") <= (
        utils.edit_distance("test", "tester") + utils.edit_distance("tester", "testing")
    )


def test_source_utils(event_source):
    assert utils.get_table(event_source) == "sample_namespace.sample_table_group_by"
    assert utils.get_topic(event_source) is None
    assert not utils.is_streaming(event_source)


def test_group_by_utils(event_group_by):
    assert not utils.get_streaming_sources(event_group_by)
