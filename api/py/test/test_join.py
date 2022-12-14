from ai.chronon.join import Join
from ai.chronon.group_by import GroupBy
from ai.chronon.api import ttypes as api

import pytest
import json


def event_source(table):
    """
    Sample left join
    """
    return api.Source(
        events=api.EventSource(
            table=table,
            query=api.Query(
                startPartition="2020-04-09",
                selects={
                    "subject": "subject_sql",
                    "event_id": "event_sql",
                },
                timeColumn="CAST(ts AS DOUBLE)",
            ),
        ),
    )


def right_part(source):
    """
    Sample Agg
    """
    return api.JoinPart(
        groupBy=api.GroupBy(
            sources=[source],
            keyColumns=["subject"],
            aggregations=[],
            accuracy=api.Accuracy.SNAPSHOT,
            backfillStartDate="2020-04-09",
        ),
    )


def test_deduped_dependencies():
    """
    Check left and right dependencies are deduped in metadata.
    """
    join = Join(
        left=event_source("sample_namespace.sample_table"),
        right_parts=[right_part(event_source("sample_namespace.another_table"))])
    assert len(join.metaData.dependencies) == 2

    join = Join(
        left=event_source("sample_namespace.sample_table"),
        right_parts=[right_part(event_source("sample_namespace.sample_table"))])
    assert len(join.metaData.dependencies) == 1


def test_additional_args_to_custom_json():
    join = Join(
        left=event_source("sample_namespace.sample_table"),
        right_parts=[right_part(event_source("sample_namespace.sample_table"))],
        team_override="some_other_team_value"
    )
    assert json.loads(join.metaData.customJson)['team_override'] == "some_other_team_value"


def test_dependencies_propagation():
    gb1 = GroupBy(
        sources=[event_source("table_1")],
        keys=["subject"],
        aggregations=[],
    )
    gb2 = GroupBy(
        sources=[event_source("table_2")],
        keys=["subject"],
        aggregations=[],
        dependencies=["table_2/ds={{ ds }}/key=value"]
    )
    join = Join(
        left=event_source("left_1"),
        right_parts=[api.JoinPart(gb1), api.JoinPart(gb2)]
    )

    actual = [
        (json.loads(dep)["name"], json.loads(dep)["spec"])
        for dep in join.metaData.dependencies
    ]
    expected = [
        ("wait_for_left_1_ds", "left_1/ds={{ ds }}"),
        ("wait_for_table_1_ds", "table_1/ds={{ ds }}"),
        ("wait_for_table_2_ds_ds_key_value", "table_2/ds={{ ds }}/key=value")
    ]
    assert expected == actual
