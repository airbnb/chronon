# Sources

Sources in Chronon define data inputs to feature computation pipelines. It's often the first thing that users will write when defining a new entity in Chronon, and getting it right is one of the most important steps in the authoring process. Defining it correctly will make everything fall into place correctly.

There are five base source types in Chronon that can be used as inputs to feature pipelines. They differ primarily in the shape of data that they ingest, and where they ingest it from.

All features are created with one of these sources as input, except for `ChainedFeatures` which use existing features as inputs.

## Overview

All sources are basically composed of the following pieces*:

1. Table that represents an offline warehouse such as hive or a topic that represents an event stream in a messaging system such as kafka
2. A `Query` component, which tells chronon which fields to extract for the computation pipeline

*External sources are the exception to the above, those are explained more below.


## Streaming EventSource

Taken from the [returns.py](https://github.com/airbnb/chronon/blob/main/api/py/test/sample/group_bys/quickstart/returns.py) example GroupBy in the quickstart tutorial.

```python
source = Source(
    events=EventSource(
        table="data.returns", # This points to the log table with historical return events
        topic="events.returns", # Streaming event 
        query=Query(
            selects=select("user_id","refund_amt"), # Select the fields we care about
            time_column="ts") # The event time
    ))
```

Key points:

* Contains a table that has historical data for the input event, and a topic that can be listened to for realtime updates
* The query specifies a few columns that we care about in our pipeline
* A time column is provided that corresponds to the event times with millisecond accuracy


## Batch EventSource

Modified from the above example.

```python
source = Source(
    events=EventSource(
        table="data.returns",
        topic=None, # This makes it a batch source
        query=Query(
            selects=select("user_id","refund_amt"),
        )
    ))
```

Key points:
* Omitting the topic turns a streaming event source into a batch event source, as the streaming input is not specified
* Features built on this source will be computed daily (as batch jobs) as new data lands in the source table
* Time column can also be omitted, since Chronon already knows the timeline along with feature change (batch updates everytime data lands daily)
* A time column could be included if you wanted offline computation to be intra-day accurate, however this should be done carefully, as online features will only be getting daily batch updates

## Streaming EntitySource

Here is an example of a streaming EntitySource, modeled after a hypothetical "users" table.

```python
user_activity = Source(entities=EntitySource(
  snapshotTable="db_snapshots.users",
  mutationTable="db_mutations.users",
  mutationTopic="events.users_mutations",
  query=Query(
            selects=select("user_id","account_created_ds","email_verified"), # Select the fields we care about
        )
)
```

In this case there would be:

1. A production users table that powers the application
2. An offline table `db_snapshots.users` that contains daily snapshots of the production table
3. A change data capture system that writes changes to the `events.users_mutations` topic, and has corresponding historical events in the `db_mutations.users` table.

As you can see, a pre-requisite to using the streaming `EntitySource` is a change capture system. [Debezium](https://debezium.io/) is one suitable solution for this piece of upstream infrastructure.

## Batch EntitySource

Taken from the [users.py](https://github.com/airbnb/chronon/blob/main/api/py/test/sample/group_bys/quickstart/users.py) example GroupBy in the quickstart tutorial.

```python
source = Source(
    entities=EntitySource(
        snapshotTable="data.users", # This points to a table that contains daily snapshots of the entire product catalog
        query=Query(
            selects=select("user_id","account_created_ds","email_verified"), # Select the fields we care about
        )
    ))
```

This is similar to the above, however, it only contains the `snapshotTable`, and not the batch and streaming mutations sources.
