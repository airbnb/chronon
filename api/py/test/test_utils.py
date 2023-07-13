import os

from ai.chronon.repo.serializer import json2thrift
from ai.chronon import utils
import ai.chronon.api.ttypes as api
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


@pytest.fixture
def group_by_requiring_backfill():
    from sample.group_bys.sample_team.sample_group_by import require_backfill
    #utils.__set_name(group_by_requiring_backfill, api.GroupBy, "group")
    return require_backfill


@pytest.fixture
def online_group_by_requiring_streaming():
    from sample.group_bys.sample_team.entity_sample_group_by_from_module import v1
    return v1


@pytest.fixture
def basic_staging_query():
    from sample.staging_queries.sample_team.sample_staging_query import v1
    return v1


@pytest.fixture
def basic_join():
    from sample.joins.sample_team.sample_join import v1
    return v1


@pytest.fixture
def never_scheduled_join():
    from sample.joins.sample_team.sample_join import never
    return never


@pytest.fixture
def consistency_check_join():
    from sample.joins.sample_team.sample_join import consistency_check
    return consistency_check


@pytest.fixture
def no_log_flattener_join():
    from sample.joins.sample_team.sample_join import no_log_flattener
    return no_log_flattener


@pytest.fixture
def label_part_join():
    from sample.joins.sample_team.sample_label_join import v1
    return v1


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


def test_dedupe_in_order():
    assert utils.dedupe_in_order([1, 1, 3, 1, 3, 2]) == [1, 3, 2]
    assert utils.dedupe_in_order([2, 1, 3, 1, 3, 2]) == [2, 1, 3]


def test_get_applicable_mode_for_group_bys(
        group_by_requiring_backfill,
        online_group_by_requiring_streaming
):
    modes = utils.get_applicable_modes(group_by_requiring_backfill)
    assert "backfill" in modes
    assert "upload" not in modes
    assert "streaming" not in modes

    modes = utils.get_applicable_modes(online_group_by_requiring_streaming)
    assert "backfill" not in modes
    assert "upload" in modes
    assert "streaming" in modes


def test_get_applicable_mode_for_staging_query(basic_staging_query):
    assert "backfill" in utils.get_applicable_modes(basic_staging_query)


def test_get_applicable_mode_for_joins(
        basic_join,
        never_scheduled_join,
        consistency_check_join,
        no_log_flattener_join,
        label_part_join
):
    modes = utils.get_applicable_modes(basic_join)
    assert "backfill" in modes
    assert "stats-summary" in modes
    assert "consistency-metrics-compute" not in modes
    assert "log-flattener" in modes  # default sample rate = 100

    modes = utils.get_applicable_modes(never_scheduled_join)
    assert "backfill" not in modes
    assert "stats-summary" not in modes
    assert "consistency-metrics-compute" not in modes
    assert "log-flattener" in modes

    modes = utils.get_applicable_modes(consistency_check_join)
    assert "consistency-metrics-compute" in modes

    modes = utils.get_applicable_modes(no_log_flattener_join)
    assert "consistency-metrics-compute" not in modes
    assert "log-flattener" not in modes

    modes = utils.get_applicable_modes(label_part_join)
    assert "consistency-metrics-compute" not in modes
    assert "label-join" in modes


def test_get_related_table_names_for_group_bys(
        group_by_requiring_backfill,
        online_group_by_requiring_streaming
):
    with open('test/sample/production/group_bys/sample_team/entity_sample_group_by_from_module.v1') as conf_file:
        json = conf_file.read()
        group_by = json2thrift(json, api.GroupBy)
        tables = utils.get_related_table_names(group_by)
        assert any(table.endswith("_upload") for table in tables)


def test_get_related_table_names_for_group_bys():
    with open('test/sample/production/group_bys/sample_team/entity_sample_group_by_from_module.v1') as conf_file:
        json = conf_file.read()
        group_by = json2thrift(json, api.GroupBy)
        tables = utils.get_related_table_names(group_by)
        # Upload is the only related table for group bys
        assert any(table.endswith("_upload") for table in tables)
        assert not any(table.endswith("_daily_stats") for table in tables)
        assert not any(table.endswith("_logged") for table in tables)
        assert not any(table.endswith("_consistency") for table in tables)
        assert not any(table.endswith("_labels") for table in tables)
        assert not any(table.endswith("_labeled") for table in tables)
        assert not any(table.endswith("_labeled_latest") for table in tables)
        assert not any(table.endswith("_bootstrap") for table in tables)


def test_get_related_table_names_for_simple_joins():
    with open('test/sample/production/joins/sample_team/sample_join.v1') as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables = utils.get_related_table_names(join)

        # Joins will have the stats-summary & log (because of default sample = 100%)
        assert any(table.endswith("_daily_stats") for table in tables)
        assert any(table.endswith("_logged") for table in tables)

        assert not any(table.endswith("_upload") for table in tables)
        assert not any(table.endswith("_consistency") for table in tables)
        assert not any(table.endswith("_labels") for table in tables)
        assert not any(table.endswith("_labeled") for table in tables)
        assert not any(table.endswith("_labeled_latest") for table in tables)
        assert not any(table.endswith("_bootstrap") for table in tables)


def test_get_related_table_names_for_label_joins():
    with open('test/sample/production/joins/sample_team/sample_label_join.v1') as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables = utils.get_related_table_names(join)

        # Joins will have the stats-summary & log (because of default sample = 100%)
        assert any(table.endswith("_daily_stats") for table in tables)
        assert any(table.endswith("_logged") for table in tables)
        # label tables should be available
        assert any(table.endswith("_labels") for table in tables)
        assert any(table.endswith("_labeled") for table in tables)
        assert any(table.endswith("_labeled_latest") for table in tables)

        assert not any(table.endswith("_upload") for table in tables)
        assert not any(table.endswith("_consistency") for table in tables)
        assert not any(table.endswith("_bootstrap") for table in tables)


def test_get_related_table_names_for_consistency_joins():
    with open('test/sample/production/joins/sample_team/sample_join.consistency_check') as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables = utils.get_related_table_names(join)

        # Joins will have the stats-summary & log (because of default sample = 100%)
        assert any(table.endswith("_daily_stats") for table in tables)
        assert any(table.endswith("_logged") for table in tables)
        # consistency tables should be available
        assert any(table.endswith("_consistency") for table in tables)

        assert not any(table.endswith("_labels") for table in tables)
        assert not any(table.endswith("_labeled") for table in tables)
        assert not any(table.endswith("_labeled_latest") for table in tables)
        assert not any(table.endswith("_upload") for table in tables)
        assert not any(table.endswith("_bootstrap") for table in tables)


def test_get_related_table_names_for_bootstrap_joins():
    with open('test/sample/production/joins/sample_team/sample_join_bootstrap.v1') as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables = utils.get_related_table_names(join)

        # Joins will have the stats-summary & log (because of default sample = 100%)
        assert any(table.endswith("_daily_stats") for table in tables)
        assert any(table.endswith("_logged") for table in tables)
        # bootstrap tables should be available
        assert any(table.endswith("_bootstrap") for table in tables)

        assert not any(table.endswith("_labels") for table in tables)
        assert not any(table.endswith("_labeled") for table in tables)
        assert not any(table.endswith("_labeled_latest") for table in tables)
        assert not any(table.endswith("_upload") for table in tables)
        assert not any(table.endswith("_consistency") for table in tables)
