import pytest, json

from ai.chronon import group_by, query
from ai.chronon.group_by import GroupBy, TimeUnit, Window, Aggregation
from ai.chronon.api import ttypes
from ai.chronon.api.ttypes import EventSource, EntitySource, Operation


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
            percentile=group_by.Aggregation(
                input_column="event_id", operation=group_by.Operation.APPROX_PERCENTILE([0.5, 0.75])
            ),
        ),
    )
    assert all([agg.inputColumn for agg in gb.aggregations if agg.operation != ttypes.Operation.COUNT])
    group_by.validate_group_by(gb)
    with pytest.raises(ValueError):
        fail_gb = group_by.GroupBy(
            sources=event_source("table"),
            keys=["subject"],
            aggregations=group_by.Aggregations(
                percentile=group_by.Aggregation(
                    input_column="event_id", operation=group_by.Operation.APPROX_PERCENTILE([1.5])
                ),
            ),
        )


def test_generic_collector():
    aggregation = group_by.Aggregation(
        input_column="test", operation=group_by.Operation.APPROX_PERCENTILE([0.4, 0.2]))
    assert aggregation.argMap == {"k": "128", "percentiles": "[0.4, 0.2]"}


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


def test_additional_metadata():
    gb = group_by.GroupBy(
        sources=[
            ttypes.EventSource(
                table="event_table1",
                query=query.Query(
                    selects=None,
                    time_column="ts"
                )
            )
        ],
        keys=["key1", "key2"],
        aggregations=[group_by.Aggregation(input_column="event_id", operation=ttypes.Operation.SUM)],
        tags={"to_deprecate": True}
    )
    assert json.loads(gb.metaData.customJson)['groupby_tags']['to_deprecate']


ratings_features = GroupBy(
    sources=[
        EntitySource(
            snapshotTable="item_info.ratings_snapshots_table",
            mutationTable="item_info.ratings_mutations_table",
            mutationTopic="ratings_mutations_topic",
            query=query.Query(
                selects={
                    "rating": "CAST(rating as DOUBLE)",
                },
                time_column="ts",
            ))
    ],
    keys=["item"],
    aggregations=[
        Aggregation(
            input_column="rating",
            operation=Operation.AVERAGE,
            windows=[Window(length=90, timeUnit=TimeUnit.DAYS)],
        ),
    ],
)


view_features = GroupBy(
    sources=[
        EventSource(
            table="user_activity.user_views_table",
            topic="user_views_stream",
            query=query.Query(
                selects={
                    "view": "if(context['activity_type'] = 'item_view', 1 , 0)",
                },
                wheres=["user != null"],
                time_column="ts",
            )
        )
    ],
    keys=["user", "item"],
    aggregations=[
        Aggregation(
            input_column="view",
            operation=Operation.COUNT,
            windows=[Window(length=5, timeUnit=TimeUnit.HOURS)],
        ),
    ],
)