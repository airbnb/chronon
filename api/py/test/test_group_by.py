from ai.chronon import query
from ai.chronon.group_by import GroupBy, TimeUnit, Window
from ai.chronon.api.ttypes import EventSource, EntitySource, Aggregation, Operation, JoinPart



ratings_features = GroupBy(
    sources=[
        EntitySource(
            snapshotTable="item_info.ratings_snapshots_table",
            mutationsTable="item_info.ratings_mutations_table",
            mutationsTopic="ratings_mutations_topic",
            query=query.Query(
                selects={
                    "rating": "CAST(rating as DOUBLE)",
                }))
    ],
    keys=["item"],
    aggregations=[
        Aggregation(operation=Operation.AVERAGE, windows=[Window(length=90, timeUnit=TimeUnit.DAYS)]),
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
                  wheres=["user != null"]
              )
          )
      ],
      keys=["user", "item"],
      aggregations=[
          Aggregation(operation=Operation.COUNT, windows=[Window(length=5, timeUnit=TimeUnit.HOURS)]),
      ],
)


from ai.chronon.join import Join, JoinPart
from ai.chronon.api.ttypes import EventSource

item_rec_features = Join(
    left=EventSource(
        table="user_activity.view_purchases",
        query=query.Query(
            start_partition='2021-06-30'
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
