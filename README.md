# Chronon: A Feature Platform for AI/ML

Chronon is a platform that abstracts away the complexity of data computation and serving for AI/ML applications. Users define features as transformation of raw data, then Chronon can perform batch and streaming computation, scalable backfills, low-latency serving, guaranteed correctness and consistency, as well as a host of observability and monitoring tools.

It allows you to utilize all of the data within your organization to power your AI/ML projects, without needing to worry about all the complex orchestration that this would usually entail.

![High Level](https://chronon.ai/_images/chronon_high_level.png)


## Benefits of Chronon over other approaches

Chronon offers the most value to AI/ML practitioners who are trying to build "online" models that are serving requests in real-time as opposed to batch workflows.

Without Chronon, engineers working on these projects need to figure out how to get data to their models for training/eval as well as production inference. As the complexity of data going into these models increases (multiple sources, complex transformation such as windowed aggregations, etc), so does the infrastructure challenge of supporting this data plumbing.

Generally, we observed ML practitioners taking one of two approaches:

### The log-and-wait approach

With this approach, users start with the data that is available in the online serving environment from which the model inference will run. Log relevant features to the data warehouse. Once enough data has accumulated, train the model on the logs, and serve with the same data.

Pros:
- Features used to train the model are guaranteed to be available at serving time
- The model can access service call features 
- The model can access data from the the request context


Cons:
- It might take a long to accumulate enough data to train the model
- Performing windowed aggregations is not always possible (running large range queries against production databases doesn't scale, same for event streams)
- Cannot utilize the wealth of data already in the data warehouse
- Maintaining data transformation logic in the application layer is messy

### The replicate offline-online approach

With this approach, users train the model with data from the data warehouse, then figure out ways to replicate those features in the online environment.

Pros:
- You can use a broad set of data for training
- The data warehouse is well suited for large aggregations and other computationally intensive transformation

Cons:
- Often very error prone, resulting in inconsistent data between training and serving
- Requires maintaining a lot of complicated infrastructure to even get started with this approach, 
- Serving features with realtime updates gets even more complicated, especially with large windowed aggregations
- Unlikely to scale well to many models

**The Chronon approach** 

With Chronon you can use any data available in your organization, including everything in the data warehouse, any streaming source, service calls, etc, with guaranteed consistency between online and offline environments. It abstracts away the infrastructure complexity of orchestrating and maintining this data plumbing, so that users can simply define features in a simple API, and trust Chronon to handle the rest.

## Platform Features

### Online Serving

Chronon offers an API for realtime fetching which returns up-to-date values for your features. It supports:

- Managed pipelines for batch and realtime feature computation and updates to the serving backend
- Low latency serving of computed features 
- Scalable for high fanout feature sets

### Backfills 

ML practitioners often need historical views of feature values for model training and evaluation. Chronon's backfills are:

- Scalable for large time windows
- Resilient to highly skewed data
- Point-in-time accurate such that consistency with online serving is guaranteed

### Observability, monitoring and data quality

Chronon offers visibility into:

- Data freshness - ensure that online values are being updated in realtime
- Online/Offline consistency - ensure that backfill data for model training and evaluation is consistent with what is being observed in online serving 

### Complex transformations and windowed aggregations

Chronon supports a range of aggregation types. For a full list see the documentation (here)[https://chronon.ai/Aggregations.html].

These aggregations can all be configured to be computed over arbitrary window sizes.

## Examples

Below are some simple code examples meant to illustrate the main APIs that Chronon offers. These examples are based off the code in (the Quickstart Guide)[api/py/quickstart], please see that section for more details on how to actually run these.

### Streaming features

Here is an example streaming feature based off of a "purchases" event for a theoretical online retailer. This data source is comprised of a kafka topic as well as a corresponding log table in Hive:

```python
source = Source(
    events=EventSource(
        table="purchases", # This points to the log table with historical purchase events
        topic="events/purchase_events", # The streaming source topic
        query=Query(
            selects=select("user_id","purchase_price"), # Select the fields we care about, in this case just the user_id (pk for aggregation), and purchase price (field for aggregation)
            time_column="ts") # The event time
    ))

window_sizes = [Window(length=day, timeUnit=TimeUnit.DAYS) for day in [3, 30, 90]] # Define some window sizes

GroupBy(
    sources=[source],
    keys=["user_id"], # We are aggregating by user
    aggregations=[Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=window_sizes
        ), # The sum of purchases prices by user in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=window_sizes
        ), # The count of purchases by user in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ) # The average purchases by user in various windows
    ],
)
```

This `GroupBy` would create a total of 9 different features -- 3, 30 and 90 day aggregations across the three different operations.

### Batch features

Here is an example batch feature based off of a "user" table, again for a theoretical online retailer. This data source is Hive table that contains snapshots of all users on the platform, with some relevant information about each user that we simply wish to extract as features.

The primary key for this GroupBy is the same as the primary key of the source table. Therefore,
it doesn't perform any aggregation.

```python
source = Source(
    entities=EntitySource(
        snapshotTable="products", # This points to a table that contains daily snapshots of the entire product catalog
        query=Query(
            selects=select("user_id","account_created_ds","email_verified"), # Select the fields we care about
            time_column="ts") # The event time
    ))

GroupBy(
    sources=[source],
    keys=["user_id"], # Primary key is the same as the primary key for the source table
    aggregations=None # In this case, there are no aggregations or windows to define
) 
```

This `GroupBy` creates two features which are extracted directly from the underlying table: `"account_created_ds", "email_verified"`.

### Combining these features together and backfilling

The `Join` API is responsible for:

1. Combining many features together into a wide view (hence the name Join).
2. Defining the primary keys and timestamps for which feature backfills should be performed. Chronon can then guarantee that feature values are correct as of this timestamp.
3. Performing scalabe backfills

Here is an example that joins the above `GroupBy`s using the `checkout` event as the left source. Using checkouts means that every row of output in the backfill corresponds to a checkout event, and features are all computed for the user and the exact timestamp of that checkout event. 

```python
source = Source(
    events=EventSource(
        table="checkout", 
        query=Query(
            selects=select("user_id"), # The primary key used to join various GroupBys together
            time_column="ts") # The event time used to compute feature values as-of
    ))

Join(  
    left=source,
    right_parts=[JoinPart(group_by=group_by) for group_by in [purchases_v1, users]] # Include the GroupBys defined above
    start_date="2023-01-01"
)
```

This join can now be run with a simple CLI call. See (Quickstart)[api/py/quickstart] for more details.

The output of the backfill would contain the user_id and ts columns from the left source, as well as the 11 feature columns from the two GroupBys.

Feature values would be computed for each user_id and ts on the left side, with guaranteed temporal accuracy. So, for example, if one of the rows on the left was for `user_id = 123` and `ts = 2023-10-01 10:11:23.195`, then the `purchase_price_avg_30d` feature would be computed for that user with a precise 30 day window ending on that timestamp.

### Fetching Data

With the above entities defined, users can now easily fetch feature vectors with a simple API call.

This is an example of using the python CLI which calls the Java fetcher under the hood, intended for manual testing. For production, the Java client is usually embedded directly into services.

```bash
python3 run.py --mode=fetch -k '{"user_id":"123"}' -n retail_example/training_set -t join

> '{"purchase_price_avg_3d":14.3241, "purchase_price_avg_30d":11.89352, ...}'
```

## Contributing

We welcome contributions to the Chronon project! Please read our (CONTRIBUTING.md)[CONTRIBUTING.md] for details.

## Support

Use the GitHub issue tracker for reporting bugs or feature requests.
Join our community channels for discussions, tips, and support. TODO: create and link channel here.
