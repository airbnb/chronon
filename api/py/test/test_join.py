from ai.chronon.join import Join
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
