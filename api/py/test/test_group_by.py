"""
Module for testing code paths not covered in normal compile.
"""
from ai.zipline import group_by
from ai.zipline.api import ttypes

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
    assert group_by.Select('subject', event="event_expr") == {"subject": "subject", "event": "event_expr"}


def test_aggregation_util(sum_op):
    """
    Test aggregation util to fill values.
    """
    agg = ttypes.Aggregation(
        operation=sum_op,
    )
    filled = group_by.Aggregations(event=agg)
    # Note it actually mutates the input
    assert agg.inputColumn == 'event'
    assert filled[0].inputColumn == 'event'
    agg = ttypes.Aggregation()
    pre_op = agg.operation
    filled = group_by.Aggregations(event=agg)
    assert agg.inputColumn == 'event' and agg.operation != pre_op


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
