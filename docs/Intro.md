# Introduction

Chronon is a feature engineering framework.

Feature Serving
Feature Backfilling
Realtime Feature Updates
Point-In-Time-Correct Feature Backfills
Powerful Aggregations

# Components

## Source

### Entities

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

- **standalone backfilled** - daily snapshots of aggregate values. The result is a date partitioned Hive Table where each partition contains aggregates as of that day, for each user that has row in the largest window ending that day.

- **backfilled against another source** - see [Join](#join) below. Most commonly used to enrich labelled data with aggregates coming from many different sources at once. 

There are a few requirements

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