# Offline Data Management

1. [Introduction](#Introduction)
2. [Getting Started](#Getting-Started)
4. [How does it work](#How-does-it-work)

## Introduction

ODM (offline data management) is a Chronon component that improves the overall experience of creating and managing offline datasets that are powering your ML workflows.

It consists of 3 core building blocks:

1. **Logging** - for capturing online production features
   Feature keys & values are now automatically logged and processed into offline tables for all online Chronon requests. You can utilize the offline log table for many purposes, such as ML observability and auto-retraining.

2. **Bootstrap** - for producing feature sets from across sources & environments  
   You can now create a new training set table from multiple data sources. For example, you can utilize logging for production features and backfill for experimental features; you can reuse backfilled data across multiple runs; you can even share feature values produced by other folks. No more backfills if the data is already available somewhere!

3. **Label computation** - for attaching labels to features to form the full training set
   You can now add labels to your features! Chronon now supports a wide range of labeling computation patterns, including both pre-aggregated labels, or labels that require windowed aggregations

## Why is this important 
ML feature engineering is an iterative process with two phases:

1. Initial phase: when a model and its training dataset is built from a scratch
2. Continuous improvement phase: when we build on top of data collected from the existing production model and continue to evolve with new features

And with ODM, Chronon focuses equally on the ML continuous improvement phase:

1. For online models in production,  **log-based training data generation** is the preferred way as opposed to offline recomputation.
   We want to capture production feature values for future model retrains, which provides better online-offline consistency and computational efficiency.

2. To experiment with new features, people rely on backfills, and they need to perform **stitching between logs and backfills**
   to create the full feature set.

3. Finally, labeling data was a missing piece in Chronon, and users had to build & maintain isolated data pipelines to
   **join features data with labels data**.

With ODM, Chronon now combines **logs, backfills**, and **labeling** in a unified API.

![architecture](./images/odm_arch.png)

## Getting Started
ODM is fully integrated with Chronon API, with a few small tweaks.

**Generate feature data from online logging and offline backfills**

```python
my_model = Join(
    left=<my_driver_table>,
    right_parts=<my_features>
    # Define row-level identifiers for your offline dataset
    row_ids=["my_event_id"],
    # Send row_ids during online fetching to make the logs joinable offline
    online_external_parts=[
      ExternalPart(ContextualSource(
         fields=[("my_event_id", DataType.STRING)]
      )
    ],
    # Use log table as a bootstrap source to skip offline backfill
    bootstrap_from_log=True
)

```

**Generate training data by combining features & labels**

```python
my_model = Join(
   ...,
   # Define label table and add it to the training set 
   label_part=LabelPart(
      labels=[JoinPart(group_by=GroupBy(
         name="my_label",
         sources=[HiveEntitySource(
            namespace="db_name",
            table="<my_label_table>",
            query=Query(selects=select(
               label_col="label_col",
               entity_id="entity_id"))
         )],
         aggregations=None,
         keys=["<entity_id>"]
      ))],
      left_start_offset=30,
      left_end_offset=0,
   )
)

```

### Examples
- [Chronon Conf](https://git.musta.ch/airbnb/ml_models/blob/master/zipline/joins/payments/payout/v2.py) Payments ML leveraged ODM to create a training dataset to train a model that detects fraudulent transactions when a payout happens
- [Chronon Conf](https://git.musta.ch/airbnb/ml_models/blob/master/zipline/joins/trust_v21/reservation_event.py) Trust Offline Risk leveraged ODM to create a training dataset to train a model that detects potential offline incidents when a booking happens

## How does it work

### Logging

**How is logging enabled?**

When you fetch a chronon join from a service, we automatically log the full traffic into a jitney event, and maintain a
data pipeline to convert the raw jitney event into a flattened hive table, where each column represents a key or feature in the join.

**How do I find the table name?**
The table name is similar to the name of the join table, with an extra _logged suffix. For example,
[payments_ml.payments_payout_v2_online_logged](https://dataportal.airbnb.tools/graph/nodes/hive/table/payments_ml.payments_payout_v2_online_logged)
[for join payments.payout.v2](https://git.musta.ch/airbnb/ml_models/blob/master/zipline/joins/payments/payout/v2.py)

### Bootstrap
**How does bootstrap work?**

When a join job is run, it backfills all features against the keys/timestamp of the left table. When bootstrap parts
are defined in the join, before it runs the backfill step, it joins the left table with the bootstrap tables on the
row_ids (a new param introduced by ODM) columns. Then for each following backfill, it checks whether the rows on the
left are already covered by the bootstrap. If so, then a backfill is skipped; if not, then a backfill is scheduled.

Bootstrap can happen both for a subset of rows and a subset of columns. Chronon will automatically backfill the rest
of rows or columns that are not covered by bootstrap.

**What is a bootstrap table?**

Bootstrap table is a precomputed table which contains precomputed feature values for (subsets of) left table. We expect
that it contains the row_id columns in order to be joined with the left table. Besides, clients should know how to match
the columns of the bootstrap table to the columns of the join to be bootstrapped.

### Label Join
**What does label join job do**

The label join allows generation of offline labeled datasets with associated features. Once regular join backfilling
processes are complete, you can initiate a Label Join job to incorporate available labels into the feature set,
resulting in a comprehensive training dataset. This enables access to all labeled features, including a 'Labeled Latest'
table that conveniently provides the most recent available label version for analysis purposes.

We provide support for both non-aggregated labels and labels that require aggregation. Label aggregations function
similarly to regular feature group-by aggregations, and existing operations can also be applied to labels.

**How does label join work?**
Including a label_part to your join now creates a new set of workflows to create label tables and a labeled training
set view for the join. Internally, chronon (1) joins the label source table with the left table to create a materialized
label table, and (2) maintains a Hive view that joins the materialized label table (which doesn’t have feature data)
with the feature table. The (2) result is not materialized for cost efficiency consideration. Label join will also have
a dedicated DAG for label computation. In another word, for offline pipelines, you will have two workflows, one for join
backfill and the other for label join job.

**What do output tables look like?**
```
 # output tables
 # 1. Regular join feature backfill table
 # 2. Joined view with features and labels. On the fly and not materialized
 # 3. Joined view with feature and latest labels available. On the fly and not materialized
 my_team_my_join_v1 
 my_team_my_join_v1_labeled
 my_team_my_join_v1_labeled_latest

# sample schema of my_team_my_join_v1_labeled

                  Column                   |  Type   | Extra | Comment
-------------------------------------------+---------+-------+---------
 key_1                                     | bigint  |       |key
 date_timestamp                            | varchar |       |date
 key_2                                     | bigint  |       |key
 key_3                                     | bigint  |       |key
 feature_col_1                             | varchar |       |feature
 feature_col_2                             | varchar |       |feature
 feature_col_3                             | varchar |       |feature
 label_col_1                               | integer |       |label
 label_col_2                               | integer |       |label
 label_col_3                               | integer |       |label
 label_ds                                  | varchar |       |label version

# sample schema of my_team_my_join_v1_labeled_latest. Same as above. 
# If a particular date do have multiple label versions like 2023-01-24, 2023-02-01, 2023-02-08, only the latest label would show up in this view which is .

                  Column                   |  Type   | Extra | Comment
-------------------------------------------+---------+-------+---------
 key_1                                     | bigint  |       |key
 date_timestamp                            | varchar |       |ds
 key_2                                     | bigint  |       |key
 key_3                                     | bigint  |       |key
 feature_col_1                             | varchar |       |feature
 feature_col_2                             | varchar |       |feature
 feature_col_3                             | varchar |       |feature
 label_col_1                               | integer |       |label
 label_col_2                               | integer |       |label
 label_col_3                               | integer |       |label
 label_ds                                  | varchar |       |label version
```