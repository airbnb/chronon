# Introduction

Chronon is a feature engineering framework that allows you to compute and store features for offline model training from large scale raw data and online feature serving with low-latency based on a unified feature definition.

Chronon integrates with airflow and spark to automatically generate feature computation pipelines. We can utilize kafka and hive as data sources that power these spark pipelines.

Chronon supports:

- Defining features over data sources that
  - Require real-time aggregations over raw data
  - Midnight accurate aggregation over raw data
  - Or contain pre-aggregated data.
- Powerful [Aggregation](./Aggregations.md) primitive with windowing, bucketing and auto-unpacking support over arbitrary complex/nested data types.
- Spark Sql Expressions with udf & built-in function [library](https://spark.apache.org/docs/latest/api/sql/index.html) for projections (select clauses) or filtering (where clauses).
- Computing features in realtime based on traditional event streams or change-capture streams of databases.
- Backfilling features without waiting to accumulate logs.
- Data source types as first-class citizens. Data source type refers to patterns in which new data is ingested into the warehouse's partitions.
  - Entites - new partitions contain snapshot of the state of an entity - like user bank balances. Snapshots of db tables and dimension tables are examples.
  - Events - new partitions contain all the events that occureed in the time duration of the partition value (`date`, `hour` etc.,)
  - Cumulative Events - new partitions contains all events that have occured from the beginning of time. So newer partition are simply a superset of older partitions and hence it is sufficient to always read just the latest partition.
- Arbitrary spark queries to implement complex logic to prepare data for non-realtime accurate features.
- Idempotent computation - new runs automatically fill all partitions starting from last filled partition. This is counter to the airflow convention of each run filling only the partition of that day.

Chronon comes packaged with tools to manage a repository of feature definitions. `explore.py`, `compile.py` and `run.py` are the primary tools provided via the `chronon-ai` pip package for discovering, validating and running feature definitions respectively. The repository has a notion of a `team` that can own feature definitions and configure the execution parameters of feature computation pipelines.

## Example

Let's say you want to enrich a table of observations with two new columns representing

1. User's count of views of items in the last 5 hours - from a view event source.
2. Item's average rating in the last 90 days - from a database's rating table.

The table of observations might look like

| user_id |  item_id  |     timestamp     |
|---------|-----------|-------------------|
|  alice  |  pizza    |  2021-09-30 5:24  |
|   bob   | ice-cream |  2021-10-15 9:18  |
|  carl   |  pizza    |  2021-11-21 7:44  |

You would expect an output that is enriced with features that looks like

| user_id |  item_id  |      timestamp      | views_count_5h  | avg_rating_90d |
|---------|-----------|---------------------|-----------------|----------------|
|  alice  |  pizza    |    2021-09-30 5:24  |       10        |       3.7      |  
|   bob   | ice-cream |    2021-10-15 9:18  |        7        |       4.5      |
|  carl   |  pizza    |    2021-11-21 7:44  |       35        |       2.1      |

Users use this data, typically, as features, to train machine learning models. These models also require these aggregations while serving to make predictions.
So online we get a request that translates to something like - "produce the view count in the last 5hrs, and average rating in the last 90 days, **ending now** for a user named Bob".

You would express the computation in Chronon using `GroupBy` and `Join`

- Average rating of the item in the last 90 days as a `GroupBy`

```python
ratings_features = GroupBy(
    sources=[
        EntitySource(
            snapshotTable="item_info.ratings_snapshots_table",
            mutationsTable="item_info.ratings_mutations_table",
            mutationsTopic="ratings_mutations_topic",
            query=query.Query(
                selects={
                    "rating": "CAST(rating as DOUBLE)"
                }))
    ],
    keys=["item"],
    aggregations=[
        Aggregation(
            operation=Operation.AVERAGE,
            windows=[Window(length=90, timeUnit=TimeUnit.DAYS)])
    ])
```

- Number of times the user viewed the item in the last 5 hours as a `GroupBy`

```python
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
            ))
    ],
    keys=["user", "item"],
    aggregations=[
        Aggregation(
            operation=Operation.COUNT, 
            swindows=[Window(length=5, timeUnit=TimeUnit.HOURS)]),
    ])
```

- `GroupBy` is essentially a collection of features that are aggregated from sources with similar data. You can put features from different GroupBy's together using a `Join`.

```python
item_rec_features = Join(
    left=EventSource(
        table="user_activity.view_purchases",
        query=query.Query(
            start_partition='2021-06-30'
        )
    ),
    ## keys are automatically mapped from left to right_parts
    right_parts=[JoinPart(groupBy = view_features), JoinPart(groupBy = ratings_features)],
)
```

Note: Chronon uses a combination of spark sql expressions and a powerful python API to allow users to define complex feature pipelines. This means you will have access to the spark expression language, built-in functions and UDFs along with Chronon's aggregation engine.

Full code [example](https://gist.github.com/nikhilsimha/13cf46b93116bc3b0b08b4adc1483bd1)

## Components

## Source

Source in chronon refers to a logical group of underlying physical data sources.

### Entities

In the average rating example above we have a `snapshotTable`, a `mutationTable` and a `mutationTopic` that are specified together as an `EntitySource`.

`snapshotTable` is a daily-partitioned hive table with each partition containing a snapshot of the `ratings` transactional database table as of midnight. The transactional table might have been used to store and serve ratings data in application servers. One would use a tool like [Sqoop](https://sqoop.apache.org/) and [Airflow](https://airflow.apache.org/) to setup a regular pipeline to capture a snapshot of the transactional table into hive.

`mutationsTopic` is a kafka topic containing the mutations to the rows of the transactional table. One could attach a tool like [Debezium](https://debezium.io/) to a transactional databases like mysql, postgres, oracle or sql server. Debezium will then begin streaming  mutations into kafka topics.

`mutationTable` is also a daily-partitioned hive table, but with each partition containing a log of mutations that occured on the transactional table during that particular day.

To visualize lets say we have a ratings table that looks like so

| user_id |   item_id |  rating | rating_timestamp |
|---------|-----------|---------|------------------|
|  alice  |    pizza  |    5    |  2021-08-16 5:24 |
|   bob   | ice_cream |    3    |  
|  carl   | ice_cream |    4    |
| dominic |    pizza  |    1    |  2021-10-21 7:44 |
|  eva    |    pizza  |    4    |  2021-10-21 7:44 |
|  fred   | ice_cream |    4    |  2021-10-21 7:44 |

### Events

### Cumulative Events

### Start & End Partitions

---

## GroupBy
A **GroupBy** is a group of [Aggregations](Aggregations.md) computed from a `Source` or similar `Source`s of data.

Consider the following group of aggregations from an user purchase stream `(user, credit_card, merchant, price, timestamp)` that are `key`-ed on user.

- `average` of purchase prices of a user in last 7d, 30d & 90d windows.
- `top_k(10)` purchase prices of a user in the last 365d window
- `unique_count` of merchants of a user - in all history.
- `average` purchase price of a user **bucketed** by merchant - result is a map of merchant to average purchase price.

The above example illustrates the computation of aggregates in several contexts.

- **served online** in **realtime** - you can utilize the Chronon client (java/scala) to query for the aggregate values as for **now**. The client would reply with realtime updated aggregate values. This would require a *stream* of user purchases and also a warehouse (hive) *table* of historical user purchases.

- **served online** as **midnight snapshots** - you can utilize the client to query for the aggregate values as of **today's midnight**. The values are only refreshed every midnight. This would require just the warehouse (hive) table of historical user purchases that receives a new partition every midnight.
  - *Note: Users can configure accuracy to be midnight or realtime*

- **standalone backfilled** - daily snapshots of aggregate values. The result is a date partitioned Hive Table where each partition contains aggregates as of that day, for each user that has row in the largest window ending that day.

- **backfilled against another source** - see [Join](#join) below. Most commonly used to enrich labelled data with aggregates coming from many different sources & GroupBy's at once.

**selecting the right Source for your `GroupBy`** is a crucial first step to correctly defining a `GroupBy`. See the section below, `Sources` for more info on the options and when to use each.

Often, you might want to chain together aggregations (i.e. first run `LAST` then run `SUM` on the output). This can be achieved by using the output of one `GroupBy` as the input to the next.

### Aggregations

#### Windows

#### Buckets

#### Auto-Flattening

### Accuracy

### GroupBy Online (Serving)

---

## Join

### Scan Logic Table

### Join Serving

---

## Staging Query