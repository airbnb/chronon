# Quickstart

This section walks you through the steps to create a training dataset with Chronon, using a fabricated underlying raw dataset.

Includes:
- Example implementation of the main API components for defining features - `GroupBy` and `Join`.
- The workflow for authoring these entities.
- The workflow for backfilling training data.
- 

Does not include:
- A deep dive on the various concepts and terminologies in Chronon. For that, please see the [Introductory](https://chronon-ai.pages.dev/Introduction) documentation.
- Running streaming jobs and online serving of data (this only covers offline training data).

## Requirements

- Docker

## Setup

To get started with the Chronon, all you need to do is download the [docker-compose.yml](docker-compose.yml) file and run it locally:

```bash
curl -o docker-compose.yml https://github.com/airbnb/chronon/blob/master/docker-compose.yml
docker-compose up
```

You're now ready to proceed with the tutorial.

## Introduction

In this example, let's assume that we're a large online retailer, and we've detected a fraud vector based on users making purchases and later returning items. We want to train a model that will be called when the **checkout** flow commences, that will predict whether this transaction is likely to result in a fraudulent return.

## Raw data sources

Fabricated raw data is included in the [data](api/py/test/sample/data) directory. It includes four tables:

1. Users - includes basic information about users such as account created date; modeled as a batch data source that updates daily
2. Purchases - a log of all purchases by users; modeled as a log table with a streaming (i.e. Kafka) event-bus counterpart
3. Returns - a log of all returns made by users; modeled as a log table with a streaming (i.e. Kafka) event-bus counterpart
4. Checkouts - a log of all checkout events; **this is the event that drives our model predictions**

### 1. Setup the sample chronon repo and cd into the directory

In a new terminal window, run:

```shell
docker-compose exec main bash
```

This will open a shell within the chronon docker container.

## Chronon Development

Now that the setup steps are complete, we can start creating and testing various Chronon objects to define transformation and aggregations, and generate data.

### Step 1 - Define some features

Let's start with three feature sets, built on top of our raw input sources.

**Note: These python definitions are already in your `chronon` image. There's nothing for you to run until [Step 3 - Backfilling Data](#step-3---backfilling-data) when you'll run computation for these definitions.**

**Feature set 1: Purchases data features**

We can aggregate the purchases log data to the user level, to give us a view into this user's previous activity on our platform. Specifically, we can compute `SUM`s `COUNT`s and `AVERAGE`s of their previous purchase amounts over various windows.

Becuase this feature is built upon a source that includes both a table and a topic, its features can be computed in both batch and streaming.

```python
source = Source(
    events=EventSource(
        table="data.purchases", # This points to the log table with historical purchase events
        topic="events/purchase_events", # The streaming source topic
        query=Query(
            selects=select("user_id","purchase_price"), # Select the fields we care about
            time_column="ts") # The event time
    ))

window_sizes = [Window(length=day, timeUnit=TimeUnit.DAYS) for day in [3, 14, 30]] # Define some window sizes to use below

v1 = GroupBy(
    sources=[source],
    keys=["user_id"], # We are aggregating by user
    aggregations=[Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=window_sizes
        ), # The sum of purchases prices in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=window_sizes
        ), # The count of purchases in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ) # The average purchases by user in various windows
    ],
)
```

See the whole code file here: [purchases GroupBy](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/group_bys/quickstart/purchases.py). This is also in your `chronon` directory downloaded by `init.sh`. We'll be running computation for it and the other GroupBys in [Step 3 - Backfilling Data](#step-3---backfilling-data). 

**Feature set 2: Returns data features**

We perform a similar set of aggregations on returns data in the [returns GroupBy](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/group_bys/quickstart/returns.py). The code is not included here because it looks similar to the above example.

**Feature set 3: User data features**

Turning User data into features is a littler simpler, primarily because there are no aggregations to include. In this case, the primary key of the source data is the same as the primary key of the feature, so we're simple extracting column values rather than perform aggregations over rows:

```python
source = Source(
    entities=EntitySource(
        snapshotTable="data.users", # This points to a table that contains daily snapshots of the entire product catalog
        query=Query(
            selects=select("user_id","account_created_ds","email_verified"), # Select the fields we care about
        )
    ))

v1 = GroupBy(
    sources=[source],
    keys=["user_id"], # Primary key is the same as the primary key for the source table
    aggregations=None # In this case, there are no aggregations or windows to define
) 
```

Taken from the [users GroupBy](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/group_bys/quickstart/users.py).


### Step 2 - Join the features together

Next, we need the features that we previously defined backfilled in a single table for model training. This can be achieved using the Join API.

For our use case, it's very important that features are computed as of the correct timestamp. Because our model runs when the checkout flow begins, we'll want to be sure to use the corresponding timestamp in our backfill, such that features values for model training logically match what the model will see in online inference.

`Join` is the API that drives feature backfills for training data. It primarilly performs the following functions:

1. Combines many features together into a wide view (hence the name Join).
2. Defines the primary keys and timestamps for which feature backfills should be performed. Chronon can then guarantee that feature values are correct as of this timestamp.
3. Performs scalabe backfills

Here is the definition that we would use.

```python
source = Source(
    events=EventSource(
        table="data.checkouts", 
        query=Query(
            selects=select("user_id"), # The primary key used to join various GroupBys together
            time_column="ts",
            ) # The event time used to compute feature values as-of
    ))

v1 = Join(  
    left=source,
    right_parts=[JoinPart(group_by=group_by) for group_by in [purchases_v1, refunds_v1, users]] # Include the three GroupBys
)
```

Taken from the [training_set Join](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/joins/quickstart/training_set.py). 

The `left` side of the join is what defines the timestamps and primary keys for the backfill (notice that it is built on top of the `checkout` event, as dictated by our use case).

Note that this `Join` combines the above three `GroupBy`s into one data definition. In the next step, we'll run the command to execute computation for this whole pipeline.

### Step 3 - Backfilling Data

Once the join is defined, we compile it using this command:

```shell
compile.py --conf=joins/quickstart/training_set.py
```

This converts it into a thrift definition that we can submit to spark with the following command:


```shell
run.py --conf production/joins/quickstart/training_set.v1
```


The output of the backfill would contain the user_id and ts columns from the left source, as well as the 11 feature columns from the three GroupBys that we created.

Feature values would be computed for each user_id and ts on the left side, with guaranteed temporal accuracy. So, for example, if one of the rows on the left was for `user_id = 123` and `ts = 2023-10-01 10:11:23.195`, then the `purchase_price_avg_30d` feature would be computed for that user with a precise 30 day window ending on that timestamp.

You can now query the backfilled data using the spark sql shell:

```shell
spark-sql
```

And then: 

```sql
spark-sql> SELECT * FROM default.quickstart_training_set_v1 LIMIT 100;
```

You can run:

```shell
spark-sql> quit;
```

To exit the sql shell.

## Online Flows

Now that we've created a join and backfilled data, the next step would be to train a model. That is not part of this tutorial, but assuming it was complete, the next step after that would be to productionize the model online. To do this, we need to be able to fetch feature vectors for model inference. That's what this next section covers.

### Uploading data

In order to serve online flows, we first need the data uploaded to the online KV store. This is different than the backfill that we ran in the previous step in two ways:

1. The data is not a historic backfill, but rather the most up-to-date feature values for each primary key.
2. The datastore is a transactional KV store suitable for point lookups. We use MongoDB in the docker image, however you are free to integrate with a database of your choice.


Upload the purchases GroupBy:

```shell
run.py --mode upload --conf production/group_bys/quickstart/purchases.v1 --ds  2023-12-01

spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
```

Upload the returns GroupBy:

```shell
run.py --mode upload --conf production/group_bys/quickstart/returns.v1 --ds  2023-12-01

spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_returns_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
```

### Upload Join Metadata

If we want to use the `FetchJoin` api rather than `FetchGroupby`, then we also need to upload the join metadata:

```bash
run.py --mode metadata-upload --conf production/joins/quickstart/training_set.v2
```

This makes it so that the online fetcher knows how to take a requests for this join and break it up into individual GroupBy requests, returning the unified vector, similar to how the Join backfill produces the wide view table with all features.

### Fetching Data

With the above entities defined, you can now easily fetch feature vectors with a simple API call.

Fetching a join:

```bash
run.py --mode fetch --type join --name quickstart/training_set.v2 -k '{"user_id":"5"}'
```

You can also fetch a single GroupBy (this would not require the Join metadata upload step performed earlier):

```bash
run.py --mode fetch --type group-by --name quickstart/purchases.v1 -k '{"user_id":"5"}'
```

For production, the Java client is usually embedded directly into services.

```Java
Map<String, String> keyMap = new HashMap<>();
keyMap.put("user_id", "123");
Fetcher.fetch_join(new Request("quickstart/training_set_v1", keyMap))
// sample response 
> '{"purchase_price_avg_3d":14.3241, "purchase_price_avg_14d":11.89352, ...}'
```

**Note: This java code is not runnable in the docker env, it is just an illustrative example.**

## Log fetches and measure online/offline consistency

As discussed in the introductory sections of this README, one of Chronon's core guarantees is online/offline consistency. This means that the data that you use to train your model (offline) matches the data that the model sees for production inference (online).

A key element of this is temporal accuracy. This can be phrased as: **when backfilling features, the value that is produced for any given `timestamp` provided by the left side of the join should be the same as what would have been returned online if that feature was fetched at that particular `timestamp`**.

Chronon not only guarantees this temporal accuracy, but also offers a way to measure it.

The measurement pipeline starts with the logs of the online fetch requests. These logs include the primary keys and timestamp of the request, along with the fetched feature values. Chronon then passes the keys and timestamps to a Join backfill as the left side, asking the compute engine to backfill the feature values. It then compares the backfilled values to actual fetched values to measure consistency.

Step 1: log fetches

First, make sure you've ran a few fetch requests. Run:

`run.py --mode fetch --type join --name quickstart/training_set.v2 -k '{"user_id":"5"}'` 

A few times to generate some fetches.

With that complete, you can run this to create a usable log table (these commands produce a logging hive table with the correct schema):

```bash
spark-submit --class ai.chronon.quickstart.online.MongoLoggingDumper --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.chronon_log_table mongodb://admin:admin@mongodb:27017/?authSource=admin
compile.py --conf group_bys/quickstart/schema.py
run.py --mode backfill --conf production/group_bys/quickstart/schema.v1
run.py --mode log-flattener --conf production/joins/quickstart/training_set.v2 --log-table default.chronon_log_table --schema-table default.quickstart_schema_v1
```

Now you can compute consistency metrics with this command:

```bash
run.py --mode consistency-metrics-compute --conf production/joins/quickstart/training_set.v2
```

This job produces two output tables:

1. `default.quickstart_training_set_v2_consistency`: A human readable table that you can query to see the results of the consistency checks.
   1. You can enter a sql shell by running `spark-sql` from your docker bash sesion, then query the table, but note that it has many columns (multiple metrics per feature).
2. `quickstart_training_set_v2_consistency_upload`: A list of KV bytes that is uploaded to the online KV store, that can be used to power online data quality monitoring flows.


## Conclusion

Using chronon for your feature engineering work simplifies and improves your ML Workflow in a number of ways:

1. You can define features in one place, and use those definitions both for training data backfills and for online serving.
2. Backfills are automatically point-in-time correct, which avoids label leakage and inconsistencies between training data and online inference.
3. Orchestration for batch and streaming pipelines to keep features up to date is made simple.
4. Chronon exposes easy endpoints for feature fetching.
5. Consistency is guaranteed and measurable.

For a more detailed view into the benefits of using Chronon, see [Benefits of Chronon documentation](https://github.com/airbnb/chronon/tree/master?tab=readme-ov-file#benefits-of-chronon-over-other-approaches).
