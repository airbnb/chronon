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

import pytest
from ai.chronon.api import ttypes as api
from ai.chronon.group_by import GroupBy
from ai.chronon.join import Derivation, Join


def event_source(table, is_cumulative=False):
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
            isCumulative=is_cumulative,
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


def join_source():
    return api.Source(
        joinSource=api.JoinSource(
            join=Join(
                left=event_source("sample_namespace.sample_table"),
                right_parts=[right_part(event_source("sample_namespace.another_table"))],
            ),
            query=api.Query(
                startPartition="2020-04-09",
                selects={
                    "subject": "subject_sql",
                    "event_id": "event_sql",
                },
                timeColumn="CAST(ts AS DOUBLE)",
            ),
        )
    )


def test_join_with_description():
    join = Join(
        left=event_source("sample_namespace.sample_table"),
        right_parts=[right_part(event_source("sample_namespace.another_table"))],
        description="Join description",
    )
    assert join.metaData.description == "Join description"


def test_join_validation_failure_cumulative_events_left():
    with pytest.raises(ValueError, match="NOT supported by Chronon"):
        join = Join(
            left=event_source(table="sample_namespace.sample_table", is_cumulative=True),
            right_parts=[right_part(event_source("sample_namespace.another_table"))],
        )


def test_join_validation_failure_join_source_left():
    with pytest.raises(ValueError, match="NOT supported by Chronon"):
        join = Join(left=join_source(), right_parts=[right_part(event_source("sample_namespace.another_table"))])


def test_deduped_dependencies():
    """
    Check left and right dependencies are deduped in metadata.
    """
    join = Join(
        left=event_source("sample_namespace.sample_table"),
        right_parts=[right_part(event_source("sample_namespace.another_table"))],
    )
    assert len(join.metaData.dependencies) == 2

    join = Join(
        left=event_source("sample_namespace.sample_table"),
        right_parts=[right_part(event_source("sample_namespace.sample_table"))],
    )
    assert len(join.metaData.dependencies) == 1


def test_additional_args_to_custom_json():
    join = Join(
        left=event_source("sample_namespace.sample_table"),
        right_parts=[right_part(event_source("sample_namespace.sample_table"))],
        team_override="some_other_team_value",
    )
    assert json.loads(join.metaData.customJson)["team_override"] == "some_other_team_value"


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
        dependencies=["table_2/ds={{ ds }}/key=value"],
    )
    join = Join(left=event_source("left_1"), right_parts=[api.JoinPart(gb1), api.JoinPart(gb2)])

    actual = [(json.loads(dep)["name"], json.loads(dep)["spec"]) for dep in join.metaData.dependencies]
    expected = [
        ("wait_for_left_1_ds", "left_1/ds={{ ds }}"),
        ("wait_for_table_1_ds", "table_1/ds={{ ds }}"),
        ("wait_for_table_2_ds_ds_key_value", "table_2/ds={{ ds }}/key=value"),
    ]
    assert expected == actual


def test_derivation():
    derivation = Derivation(name="derivation_name", expression="derivation_expression")
    expected_derivation = api.Derivation(name="derivation_name", expression="derivation_expression")

    assert derivation == expected_derivation


def test_derivation_with_description():
    derivation = Derivation(
        name="derivation_name", expression="derivation_expression", description="Derivation description"
    )
    expected_derivation = api.Derivation(
        name="derivation_name",
        expression="derivation_expression",
        metaData=api.MetaData(description="Derivation description"),
    )

    assert derivation == expected_derivation


def test_join_with_global_aggregation():
    """
    Test that joins work with global aggregations (GroupBys with empty keys).
    Global aggregations should join on system keys (partition/timestamp) only.
    """
    # Create a global aggregation GroupBy (no keys)
    global_gb = GroupBy(
        sources=[event_source("global_stats_table")],
        keys=[],  # Empty keys = global aggregation
        aggregations=[
            api.Aggregation(inputColumn="event_id", operation=api.Operation.COUNT),
            api.Aggregation(inputColumn="event_id", operation=api.Operation.SUM),
        ],
        name="global_stats",
    )

    # Create a normal GroupBy with keys for comparison
    regular_gb = GroupBy(
        sources=[event_source("user_stats_table")],
        keys=["subject"],
        aggregations=[
            api.Aggregation(inputColumn="event_id", operation=api.Operation.LAST),
        ],
        name="user_stats",
    )

    # Create a join with both global and regular aggregations
    join = Join(
        left=event_source("events_table"),
        right_parts=[
            api.JoinPart(groupBy=global_gb, prefix="global"),
            api.JoinPart(groupBy=regular_gb),  # Uses default key mapping
        ],
        name="events_with_global_and_user_stats",
    )

    # Verify the join was created successfully
    assert join is not None
    assert len(join.joinParts) == 2

    # Verify global aggregation has empty keyColumns
    assert join.joinParts[0].groupBy.keyColumns == []
    assert len(join.joinParts[0].groupBy.aggregations) == 2

    # Verify regular aggregation has keys
    assert join.joinParts[1].groupBy.keyColumns == ["subject"]
    assert len(join.joinParts[1].groupBy.aggregations) == 1
