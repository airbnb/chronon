"""
Module for testing code paths not covered in normal compile.
"""
from ai.chronon import group_by, query
from ai.chronon.api import ttypes

import pytest


@pytest.fixture
def sum_op():
    return ttypes.Operation.SUM


@pytest.fixture
def min_op():
    return ttypes.Operation.MIN


@pytest.fixture
def days_unit():
    return ttypes.TimeUnit.DAYS


@pytest.fixture
def hours_unit():
    return ttypes.TimeUnit.HOURS


def event_source(table):
    """
    Sample left join
    """
    return ttypes.EventSource(
        table=table,
        query=ttypes.Query(
            startPartition="2020-04-09",
            selects={
                "subject": "subject_sql",
                "event_id": "event_sql",
                "cnt": 1
            },
            timeColumn="CAST(ts AS DOUBLE)",
        ),
    )


def test_pretty_window_str(days_unit, hours_unit):
    """
    Test pretty window utils.
    """
    window = ttypes.Window(
        length=7,
        timeUnit=days_unit
    )
    assert group_by.window_to_str_pretty(window) == "7 days"
    window = ttypes.Window(
        length=2,
        timeUnit=hours_unit
    )
    assert group_by.window_to_str_pretty(window) == "2 hours"


def test_pretty_operation_str(sum_op, min_op):
    """
    Test pretty operation util.
    """
    assert group_by.op_to_str(sum_op) == "sum"
    assert group_by.op_to_str(min_op) == "min"


def test_select():
    """
    Test select builder
    """
    assert query.select('subject', event="event_expr") == {"subject": "subject", "event": "event_expr"}


def test_contains_windowed_aggregation(sum_op, min_op, days_unit):
    """
    Test checker for windowed aggregations
    """
    assert not group_by.contains_windowed_aggregation([])
    aggregations = [
        ttypes.Aggregation(inputColumn='event', operation=sum_op),
        ttypes.Aggregation(inputColumn='event', operation=min_op),
    ]
    assert not group_by.contains_windowed_aggregation(aggregations)
    aggregations.append(
        ttypes.Aggregation(
            inputColumn='event',
            operation=sum_op,
            windows=[ttypes.Window(length=7, timeUnit=days_unit)]
        )
    )
    assert group_by.contains_windowed_aggregation(aggregations)


def test_validator_ok():
    gb = group_by.GroupBy(
        sources=event_source("table"),
        keys=["subject"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(inputColumn="event_id", operation=ttypes.Operation.SUM),
            event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
        ),
    )
    assert all([agg.inputColumn for agg in gb.aggregations if agg.operation != ttypes.Operation.COUNT])


def test_select_sanitization():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(  # No selects are spcified
                table="event_table1",
                query=query.Query(
                    selects=None,
                    time_column="ts"
                )
            ),
            ttypes.EntitySource(  # Some selects are specified
                snapshotTable="entity_table1",
                query=query.Query(
                    selects={
                        "key1": "key1_sql",
                        "event_id": "event_sql"
                    }
                )
            )
        ],
        keys=["key1", "key2"],
        aggregations=group_by.Aggregations(
            random=ttypes.Aggregation(inputColumn="event_id", operation=ttypes.Operation.SUM),
            event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
            cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
        ),
    )
    required_selects = set(["key1", "key2", "event_id", "cnt"])
    assert set(gb.sources[0].events.query.selects.keys()) == required_selects
    assert set(gb.sources[0].events.query.selects.values()) == required_selects
    assert set(gb.sources[1].entities.query.selects.keys()) == required_selects
    assert set(gb.sources[1].entities.query.selects.values()) == set(["key1_sql", "key2", "event_sql", "cnt"])


def test_snapshot_with_hour_aggregation():
    with pytest.raises(AssertionError):
        group_by.GroupBy(
            sources=[
                ttypes.EntitySource(  # Some selects are specified
                    snapshotTable="entity_table1",
                    query=query.Query(
                        selects={
                            "key1": "key1_sql",
                            "event_id": "event_sql"
                        },
                        time_column="ts",
                    )
                )
            ],
            keys=["key1"],
            aggregations=group_by.Aggregations(
                random=ttypes.Aggregation(inputColumn="event_id", operation=ttypes.Operation.SUM, windows=[
                    ttypes.Window(1, ttypes.TimeUnit.HOURS),
                ]),
            ),
            backfill_start_date="2021-01-04",
        )
