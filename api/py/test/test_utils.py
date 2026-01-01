#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import json
import os

import pytest

import ai.chronon.api.ttypes as api
from ai.chronon import utils
from ai.chronon.api.ttypes import EntitySource, EventSource, Query, Source
from ai.chronon.repo.serializer import file2thrift, json2thrift
from ai.chronon.utils import get_dependencies, wait_for_simple_schema


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

    # utils.__set_name(group_by_requiring_backfill, api.GroupBy, "group")
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
    assert utils.edit_distance("test", "test") == 0
    assert utils.edit_distance("test", "testy") > 0
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


def test_get_applicable_mode_for_group_bys(group_by_requiring_backfill, online_group_by_requiring_streaming):
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
    basic_join, never_scheduled_join, consistency_check_join, no_log_flattener_join, label_part_join
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


def test_get_related_table_names_for_group_bys(group_by_requiring_backfill, online_group_by_requiring_streaming):
    with open("api/py/test/sample/production/group_bys/sample_team/entity_sample_group_by_from_module.v1") as conf_file:
        json = conf_file.read()
        group_by = json2thrift(json, api.GroupBy)
        tables = utils.get_related_table_names(group_by)
        assert any(table.endswith("_upload") for table in tables)


def test_get_related_table_names_for_group_bys():
    with open("api/py/test/sample/production/group_bys/sample_team/entity_sample_group_by_from_module.v1") as conf_file:
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
    with open("api/py/test/sample/production/joins/sample_team/sample_join.v1") as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables = utils.get_related_table_names(join)

        # Joins will have the stats-summary & log (because of default sample = 100%)
        assert any(table.endswith("_daily_stats") for table in tables)
        assert any(table.endswith("_logged") for table in tables)
        assert any(table.endswith("_bootstrap") for table in tables)  # always generated in backfill mode

        assert not any(table.endswith("_upload") for table in tables)
        assert not any(table.endswith("_consistency") for table in tables)
        assert not any(table.endswith("_labels") for table in tables)
        assert not any(table.endswith("_labeled") for table in tables)
        assert not any(table.endswith("_labeled_latest") for table in tables)


def test_get_related_table_names_for_label_joins():
    with open("api/py/test/sample/production/joins/sample_team/sample_label_join.v1") as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables = utils.get_related_table_names(join)

        # Joins will have the stats-summary & log (because of default sample = 100%)
        assert any(table.endswith("_daily_stats") for table in tables)
        assert any(table.endswith("_logged") for table in tables)
        assert any(table.endswith("_bootstrap") for table in tables)  # always generated in backfill mode

        # label tables should be available
        assert any(table.endswith("_labels") for table in tables)
        assert any(table.endswith("_labeled") for table in tables)
        assert any(table.endswith("_labeled_latest") for table in tables)

        assert not any(table.endswith("_upload") for table in tables)
        assert not any(table.endswith("_consistency") for table in tables)


def test_get_related_table_names_for_consistency_joins():
    with open("api/py/test/sample/production/joins/sample_team/sample_join.consistency_check") as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables = utils.get_related_table_names(join)

        # Joins will have the stats-summary & log (because of default sample = 100%)
        assert any(table.endswith("_daily_stats") for table in tables)
        assert any(table.endswith("_logged") for table in tables)
        assert any(table.endswith("_bootstrap") for table in tables)  # always generated in backfill mode
        # consistency tables should be available
        assert any(table.endswith("_consistency") for table in tables)

        assert not any(table.endswith("_labels") for table in tables)
        assert not any(table.endswith("_labeled") for table in tables)
        assert not any(table.endswith("_labeled_latest") for table in tables)
        assert not any(table.endswith("_upload") for table in tables)


def test_get_related_table_names_for_bootstrap_joins():
    with open("api/py/test/sample/production/joins/sample_team/sample_join_bootstrap.v1") as conf_file:
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


def test_tables_with_without_skip_join_parts():
    with open("api/py/test/sample/production/joins/sample_team/sample_join_bootstrap.v1") as conf_file:
        json = conf_file.read()
        join = json2thrift(json, api.Join)
        tables_without = utils.get_related_table_names(join)
        tables_with = utils.get_related_table_names(join, skip_join_parts=False)

        expected_tables = [
            "chronon_db.sample_team_sample_join_bootstrap_v1",
            "chronon_db.sample_team_sample_join_bootstrap_v1_daily_stats",
            "chronon_db.sample_team_sample_join_bootstrap_v1_logged",
            "chronon_db.sample_team_sample_join_bootstrap_v1_bootstrap",
        ]

        expected_join_parts = [
            "sample_team_sample_join_bootstrap_v1_sample_team_event_sample_group_by_v1",
            "sample_team_sample_join_bootstrap_v1_sample_team_entity_sample_group_by_from_module_v1",
        ]

        assert set(tables_without) == set(expected_tables)
        assert set(tables_with) == set(expected_tables + expected_join_parts)


@pytest.mark.parametrize(
    "materialized_group_by,table_name",
    [
        ("entity_sample_group_by_from_module.v1", "chronon_db.sample_team_entity_sample_group_by_from_module_v1"),
        ("event_sample_group_by.v1", "sample_namespace.sample_team_event_sample_group_by_v1"),
        ("group_by_with_kwargs.v1", "chronon_db.sample_team_group_by_with_kwargs_v1"),
        ("sample_chaining_group_by", "sample_namespace.sample_team_sample_chaining_group_by"),
    ],
)
def test_group_by_table_names(repo, materialized_group_by, table_name):
    gb = file2thrift(os.path.join(repo, "production/group_bys/sample_team", materialized_group_by), api.GroupBy)
    assert utils.group_by_output_table_name(gb, True) == table_name


@pytest.mark.parametrize(
    "materialized_join,table_name",
    [
        (
            "sample_chaining_join.v1",
            "chronon_db.sample_team_sample_chaining_join_v1_sample_team_sample_chaining_group_by",
        ),
        ("sample_join.v1", "sample_namespace.sample_team_sample_join_v1_sample_team_sample_group_by_v1"),
    ],
)
def test_join_part_table_names(repo, materialized_join, table_name):
    join = file2thrift(os.path.join(repo, "production/joins/sample_team", materialized_join), api.Join)
    assert utils.join_part_output_table_name(join, join.joinParts[0], True) == table_name


@pytest.fixture(params=[None, "date_column"])
def query_with_partition_column(request):
    return Query(
        partitionColumn=request.param,
        startPartition="2021-01-01",
        endPartition="2021-01-02",
    )


def test_wait_for_simple_schema_lag_zero_no_subpartition(
    query_with_partition_column: Query,
):
    table = "mytable"
    paritition_col = query_with_partition_column.partitionColumn or "ds"
    expected_spec = f"mytable/{paritition_col}={{{{ ds }}}}"
    result = wait_for_simple_schema(
        table, 0, "2021-01-01", "2021-01-02", query=query_with_partition_column
    )
    assert result["spec"] == expected_spec


def test_wait_for_simple_schema_lag_nonzero_no_subpartition(
    query_with_partition_column,
):
    table = "mytable"
    lag = 3
    partition_col = query_with_partition_column.partitionColumn or "ds"
    expected_spec = f"mytable/{partition_col}={{{{ macros.ds_add(ds, -3) }}}}"
    result = wait_for_simple_schema(
        table, lag, "2021-01-01", "2021-01-02", query=query_with_partition_column
    )
    assert result["spec"] == expected_spec


def test_wait_for_simple_schema_with_subpartition(query_with_partition_column: Query):
    table = "mytable/system=mobile"
    lag = 0
    partition_col = query_with_partition_column.partitionColumn or "ds"
    expected_spec = f"mytable/{partition_col}={{{{ ds }}}}/system=mobile"
    result = wait_for_simple_schema(
        table, lag, "2021-01-01", "2021-01-02", query=query_with_partition_column
    )
    assert result["spec"] == expected_spec


def test_get_dependencies_with_entities_mutation(query_with_partition_column: Query):
    entity = EntitySource(
        snapshotTable="snap_table",
        mutationTable="mut_table",
        query=query_with_partition_column,
    )
    src = Source(entities=entity)
    deps_json = get_dependencies(src, lag=0)
    assert len(deps_json) == 2
    deps = [json.loads(d) for d in deps_json]
    partition_col = query_with_partition_column.partitionColumn or "ds"
    expected_spec_snap = f"snap_table/{partition_col}={{{{ ds }}}}"
    expected_spec_mut = f"mut_table/{partition_col}={{{{ ds }}}}"
    assert deps[0]["spec"] == expected_spec_snap
    assert deps[1]["spec"] == expected_spec_mut


def test_get_dependencies_with_events(query_with_partition_column: Query):
    event = EventSource(table="event_table", query=query_with_partition_column)
    src = Source(events=event)
    deps_json = get_dependencies(src, lag=2)
    assert len(deps_json) == 1
    dep = json.loads(deps_json[0])
    partition_col = query_with_partition_column.partitionColumn or "ds"
    expected_spec = f"event_table/{partition_col}={{{{ macros.ds_add(ds, -2) }}}}"
    assert dep["spec"] == expected_spec


def test_external_join_part():
    """Test ExternalJoinPart class: initialization, inheritance, and attribute copying."""
    from ai.chronon.repo.external_join_part import ExternalJoinPart

    group_by = api.GroupBy(
        metaData=api.MetaData(name="sample_team.test_group_by.v1"),
        keyColumns=["key1"],
    )
    source_jp = api.JoinPart(
        groupBy=group_by,
        keyMapping={"key": "mapped_key"},
        prefix="my_prefix",
    )
    external_jp = ExternalJoinPart(source_jp, full_prefix="ext_my_prefix_test_group_by_v1")

    # Verify it's an instance of JoinPart
    assert isinstance(external_jp, api.JoinPart)
    # Verify attributes are copied
    assert external_jp.groupBy == group_by
    assert external_jp.keyMapping == {"key": "mapped_key"}
    assert external_jp.prefix == "my_prefix"
    # Verify external-specific attributes
    assert external_jp.external_join_full_prefix == "ext_my_prefix_test_group_by_v1"
    assert external_jp._source_join_part is source_jp


def test_join_part_name_external_and_regular():
    """Test join_part_name for both ExternalJoinPart and regular JoinPart."""
    from ai.chronon.repo.external_join_part import ExternalJoinPart
    from ai.chronon.utils import join_part_name

    group_by = api.GroupBy(metaData=api.MetaData(name="sample_team.my_group_by.v1"))

    # Regular JoinPart with prefix
    regular_jp = api.JoinPart(groupBy=group_by, prefix="my_prefix")
    assert join_part_name(regular_jp) == "my_prefix_sample_team_my_group_by_v1"

    # Regular JoinPart without prefix
    regular_jp_no_prefix = api.JoinPart(groupBy=group_by)
    assert join_part_name(regular_jp_no_prefix) == "sample_team_my_group_by_v1"

    # ExternalJoinPart with prefix
    external_jp = ExternalJoinPart(regular_jp, full_prefix="ext_my_prefix_source")
    assert join_part_name(external_jp) == "ext_my_prefix_source"

    # ExternalJoinPart without user prefix
    external_jp_no_prefix = ExternalJoinPart(regular_jp_no_prefix, full_prefix="ext_source")
    assert join_part_name(external_jp_no_prefix) == "ext_source"


class TestGetMaxWindowForGbInDays:
    """Tests for get_max_window_for_gb_in_days function."""

    def test_no_aggregations_returns_default(self):
        """GroupBy with no aggregations returns default of 1."""
        group_by = api.GroupBy(metaData=api.MetaData(name="test"))
        assert utils.get_max_window_for_gb_in_days(group_by) == 1

    def test_empty_aggregations_returns_default(self):
        """GroupBy with empty aggregations list returns default of 1."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 1

    def test_window_in_days(self):
        """Window specified in days returns correct value."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[api.Window(length=7, timeUnit=api.TimeUnit.DAYS)]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 7

    def test_window_in_hours(self):
        """Window specified in hours converts correctly to days."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[api.Window(length=48, timeUnit=api.TimeUnit.HOURS)]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 2

    def test_window_in_hours_rounds_up(self):
        """Window in hours that doesn't divide evenly rounds up."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[api.Window(length=25, timeUnit=api.TimeUnit.HOURS)]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 2

    def test_window_in_minutes(self):
        """Window specified in minutes converts correctly to days."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[api.Window(length=1440, timeUnit=api.TimeUnit.MINUTES)]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 1

    def test_window_in_minutes_rounds_up(self):
        """Window in minutes that doesn't divide evenly rounds up."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[api.Window(length=1500, timeUnit=api.TimeUnit.MINUTES)]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 2

    def test_multiple_windows_returns_max(self):
        """Multiple windows returns the maximum value."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[
                        api.Window(length=1, timeUnit=api.TimeUnit.DAYS),
                        api.Window(length=7, timeUnit=api.TimeUnit.DAYS),
                        api.Window(length=30, timeUnit=api.TimeUnit.DAYS),
                    ]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 30

    def test_multiple_aggregations_returns_max(self):
        """Multiple aggregations returns max across all windows."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col1",
                    operation=api.Operation.COUNT,
                    windows=[api.Window(length=7, timeUnit=api.TimeUnit.DAYS)]
                ),
                api.Aggregation(
                    inputColumn="col2",
                    operation=api.Operation.SUM,
                    windows=[api.Window(length=14, timeUnit=api.TimeUnit.DAYS)]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 14

    def test_mixed_time_units_returns_max(self):
        """Mixed time units correctly converts and returns max."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[
                        api.Window(length=60, timeUnit=api.TimeUnit.MINUTES),
                        api.Window(length=48, timeUnit=api.TimeUnit.HOURS),
                        api.Window(length=3, timeUnit=api.TimeUnit.DAYS),
                    ]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 3

    def test_small_minute_window_returns_minimum_of_one(self):
        """Very small window still returns at least 1 day."""
        group_by = api.GroupBy(
            metaData=api.MetaData(name="test"),
            aggregations=[
                api.Aggregation(
                    inputColumn="col",
                    operation=api.Operation.COUNT,
                    windows=[api.Window(length=60, timeUnit=api.TimeUnit.MINUTES)]
                )
            ]
        )
        assert utils.get_max_window_for_gb_in_days(group_by) == 1
