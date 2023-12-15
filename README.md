# Chronon: A Data Platform for AI/ML <!-- Bold. I like it. -->

Chronon is a platform that abstracts away the complexity of data computation and serving for AI/ML applications. Users define features as transformation of raw data, then Chronon can perform batch and streaming computation, scalable backfills, low-latency serving, guaranteed correctness and consistency, as well as a host of observability and monitoring tools.
<!-- I think this is I'd say one of the things we usually downplay a lot, marketing. Chronon is not necessarily AI/ML, but it shines in AI/ML. It's lambda, it fixes consistency issues with batch correction, etc... I honestly wonder what would be the greatest marketable first 5 lines that do right by what chronon is. I feel this like the abstract of the paper, gets read and repeated more times than anything else. 
-->

It allows you to utilize all of the data within your organization, from batch tables, event streams or services to power your AI/ML projects, without needing to worry about all the complex orchestration that this would usually entail.

![High Level](https://chronon.ai/_images/chronon_high_level.png)

<!-- Same with the diagram, if you were to look at that you'd think it's mostly based on group bys. Kind of overlooking the point in time correctness of it all. I know it's shitty feedback because it's hard to know what it is that I'm looking for, and really I feel we can only figure this out with all of us in a room. But just a note to may be revisit it. -->


## Platform Features

<!-- Platform Features title. We already use the word features heavily, I'd rename to something like "Why Chronon?" or "What's included?" or "What Chronon can do for you?" I don't know why I think so much in questions, but you know what I mean, features is everywhere, let's try to only use it with one semantic meaning.

Another alternative to ^ is "platform components". But most importantly would be to add a small section of what makes chronon different. 
Batch correction, online offline consistency computation, chaining, derivations, mutations support, Only backfill the necessary (ODM), etc.

For this specific section I'm leaning towards more of a components view. 
-->

### Online Serving

Chronon offers an API for realtime fetching which returns up-to-date values for your features. It supports: <!-- Realtime fetching and up-to-date is a bit redundant isn't it? -->

- Managed pipelines for batch and realtime feature computation and updates to the serving backend  <!-- I think this one is a bit cryptic -->
- Low latency serving of computed features 
- Scalable for high fanout feature sets

### Backfills 

ML practitioners often need historical views of feature values for model training and evaluation. Chronon's backfills are:

- Scalable for large time windows
- Resilient to highly skewed data
- Point-in-time accurate such that consistency with online serving is guaranteed <!--I think the why would be something leakage aware, which is worth pointing out if your target is ML practitioners. PITC is more of the what which may fly by people's head if they are not practitioners. -->
<!-- not sure how to phrase it but there's more here. They are modular (joinPart separated + ODM allows to evolve and experiment with joins) -->

### Observability, monitoring and data quality

Chronon offers visibility into:

- Data freshness - ensure that online values are being updated in realtime <!-- I think we need to invest a bit here tbh. Telemetry in general is what we offer but our current code is tied to statsd, rather OTEL. With telemetry we offer a lot more than freshness, payloads, QPS, etc) -->
- Online/Offline consistency - ensure that backfill data for model training and evaluation is consistent with what is being observed in online serving
<!-- I think this is saying generated training data and logged data are consistent, but this is not what it measures. I think the purpose of OOC is two fold:
- [Feature definition evaluation] Can we compute a certain feature online (sometimes a lag is needed because data does not come fast enough, or a stream has backpressure)
- [Monitoring / operational] Has there been an issue in our serving that prevents us from properly computing a feature set (and what is the effect). 
-->

### Complex transformations and windowed aggregations <!-- If you've been using features so far, stick to features I think this is the first mention of aggregations and comes a bit out of nowhere (unless you know that a feature is an input select, aggregation and time range, so I'd call it windowed features for now, until you introduce how an aggregation relates to a feature. -->

Chronon supports a range of aggregation types. For a full list see the documentation [here](https://chronon.ai/Aggregations.html).

These aggregations can all be configured to be computed over arbitrary window sizes.

# Quickstart

**Note: The following is only intended to work on recent versions of MacOS.**
<!-- I think if we use the components definition above, you can hook into that by calling it say: an overview of the backfilling component (or offline data generation or data generation, training data generation, etc whichever name we settle for this 
I think dbt has interesting example with a "market" of quickstarts: https://docs.getdbt.com/guides or metricflow: https://docs.getdbt.com/docs/build/build-metrics-intro 
-->
This section walks you through the steps to create a training dataset with Chronon, using a fabricated underlying raw dataset.

Includes:
- Example implementation of the main API components for defining features - `GroupBy` and `Join`.
- The workflow for authoring these entities. <!-- I think we can anchor even more to a `Join` -->
- The workflow for backfilling training data. <!-- Similarly generating a table with the feature values for the join-->

Does not include:
- A deep dive on the various concepts and terminologies in Chronon. For that, please see the [Introductory](https://chronon-ai.pages.dev/Introduction) documentation.
- Running streaming jobs and online serving of data (this only covers offline training data).


## Introduction

In this example, let's assume that we're a large online retailer, and we've detected a fraud vector based on users making purchases and later returning items. We want to train a model that will be called when the **checkout** flow commences, that will predict whether this transaction is likely to result in a fraudulent return.

## Raw data sources

Fabricated raw data is included in the [data](api/py/test/sample/data) directory. It includes four tables:

1. Users - includes basic information about users such as account created date; modeled as a batch data source that updates daily
2. Purchases - a log of all purchases by users; modeled as a log table with a streaming (i.e. Kafka) event-bus counterpart
3. Returns - a log of all returns made by users; modeled as a log table with a streaming (i.e. Kafka) event-bus counterpart
4. Checkouts - a log of all checkout events; **this is the event that drives our model predictions**

### 1. Setup the sample chronon repo and cd into the directory

<!-- I kind of want to contribute a simpler version of this if you are open to this (and if it's possible). Downloading scripts is scary 
I'm not sure how possible this is, but I'd like to rely on docker, a dockerfile and scripts that we keep in the repo. It may even help in CI and we can even use the one we have in CI technically. So I'm curious if we can merge these two so we have one less thing to worry about keeping updated. It's a gamble,  but I can probably allocate some time for this.
-->
```shell
curl -s https://chronon.ai/init.sh | $SHELL
source "$HOME/.$(echo $SHELL | awk -F/ '{print $NF}')rc"
cd ./chronon
```

This will create a directory that is pre-populated with some fake data and a functional chronon environment.
<!-- I'd also give a context on the schemas we have available so the next part is easier to understand --> 

## Chronon Development
<!-- Again, don't call them aggregations but keep consistent with features --> 
Now that the setup steps are complete, we can start creating and testing various Chronon objects to define transformation and aggregations, and generate data.

### Step 1 - Define some features

Let's start with three feature sets, built on top of our raw input sources.

**Note: These python definitions are already downloaded in your `chronon` directory by the `init.sh` script that you ran to get setup. There's nothing for you to run until [Step 3 - Backfilling Data](#step-3---backfilling-data) when you'll run these definitions.**

**Feature set 1: Purchases data features**

We can aggregate the purchases log data to the user level, to give us a view into this user's previous activity on our platform. Specifically, we can compute `SUM`s `COUNT`s and `AVERAGE`s of their previous purchase amounts over various windows.

Becuase this feature is built upon a source that includes both a table and a topic, its features can be computed in both batch and streaming.

```python
source = Source(
    events=EventSource(
        table="data.purchases", # This points to the log table with historical purchase events
        topic="events/purchase_events", # The streaming source topic <!-- I thought we were not doing streaming -->
        query=Query(
            selects=select("user_id","purchase_price"), # Select the fields we care about
            time_column="ts") # The event time
    ))

window_sizes = [Window(length=day, timeUnit=TimeUnit.DAYS) for day in [3, 14, 30]] # Define some window sizes to use below

v1 = GroupBy(
    sources=[source],
    keys=["user_id"], # We are aggregating by user <!-- Prob the most nitpicky comment, but user_id is not super descriptive, call it seller_id or buyer_id -->
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

See the whole code file here: [purchases GroupBy](api/py/test/sample/group_bys/quickstart/purchases.py). This is also in your `chronon` directory downloaded by `init.sh`. We'll be running computation for it and the other GroupBys in [Step 3 - Backfilling Data](#step-3---backfilling-data). 

**Feature set 2: Returns data features**
<!-- I think it's fair to provide a snippet in the same step, with a comment of the file at the top. (I think I've seen this fairly often but don't remember where) the idea though is that if you do then people can just go the file in the comment and you don't have to keep pointing out where they can find the whole file -->
We perform a similar set of aggregations on returns data in the [returns GroupBy](api/py/test/sample/group_bys/returns.py). The code is not included here because it looks similar to the above example.

**Feature set 3: User data features**

Turning User data into features is a littler simpler, primarily because there are no aggregations to include. In this case, the primary key of the source data is the same as the primary key of the feature, so we're simple extracting column values rather than perform aggregations over rows:

```python
source = Source(
    entities=EntitySource(
        snapshotTable="data.users", # This points to a table that contains daily snapshots of the entire product catalog <!-- is it the product, or the users? -->
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

Taken from the [users GroupBy](api/py/test/sample/group_bys/quickstart/users.py).


<!-- Again, super nitpicky, but what we are doing is materializing historic values for the features, the join is more of the means to an end, so I'd combine the next two steps in one -->
### Step 2 - Join the features together

Next, we need the features that we previously defined backfilled in a single table for model training. This can be achieved using the Join API.

For our use case, it's very important that features are computed as of the correct timestamp. Because our model runs when the checkout flow begins, we'll want to be sure to use the corresponding timestamp in our backfill, such that features values for model training logically match what the model will see in online inference.

`Join` is the API that drives feature backfills for training data. It primarilly performs the following functions:

1. Combines many features together into a wide view (hence the name Join).
2. Defines the primary keys and timestamps for which feature backfills should be performed. Chronon can then guarantee that feature values are correct as of this timestamp.
3. Performs scalable backfills 

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

Taken from the [training_set Join](api/py/test/sample/joins/quickstart/training_set.py). 

The `left` side of the join is what defines the timestamps and primary keys for the backfill (notice that it is built on top of the `checkout` event, as dictated by our use case).

Note that this `Join` combines the above three `GroupBy`s into one data definition. In the next step, we'll run the command to execute computation for this whole pipeline.

### Step 3 - Backfilling Data

Once the join is defined, we compile it using this command:

```shell
compile.py --conf=joins/quickstart/training_set.py
```

This converts it into a thrift definition that we can submit to spark with the following command:


```shell
mkdir ~/quickstart_output

run.py --mode=backfill \
--conf=production/joins/quickstart/training_set.v1 \
--local-data-path data --local-warehouse-location ~/quickstart_output \
--ds=2023-11-30
```


The output of the backfill would contain the user_id and ts columns from the left source, as well as the 11 feature columns from the three GroupBys that we created.

Feature values would be computed for each user_id and ts on the left side, with guaranteed temporal accuracy. So, for example, if one of the rows on the left was for `user_id = 123` and `ts = 2023-10-01 10:11:23.195`, then the `purchase_price_avg_30d` feature would be computed for that user with a precise 30 day window ending on that timestamp.

You can now query the backfilled data using the spark sql shell:

```shell
cd ~/quickstart_output 
~/spark-3.2.4-bin-hadoop3.2/bin/spark-sql
```

And then: 

```sql
spark-sql> SELECT * FROM default.quickstart_training_set_v1 LIMIT 100;
```
<!-- this is pretty cool, I'd a placeholder for a sample of the output so people know it ran correctly -->

### Fetching Data  <!-- Is this different from the quickstart? I thought we were just offline, I may have misread -->

With the above entities defined, users can now easily fetch feature vectors with a simple API call.

For production, the Java client is usually embedded directly into services.

```Java
Map<String, String> keyMap = new HashMap<>();
keyMap.put("user_id", "123");
Fetcher.fetch_join(new Request("quickstart/training_set_v1", keyMap))
// sample response 
> '{"purchase_price_avg_3d":14.3241, "purchase_price_avg_14d":11.89352, ...}'
```

There is also a CLI fetcher util available for easy testing and debugging.

```python
python3 run.py --mode=fetch -k '{"user_id":"123"}' -n retail_example/training_set -t join

> '{"purchase_price_avg_3d":14.3241, "purchase_price_avg_14d":11.89352, ...}'
```

**Note that for these fetcher calls to work, you would need to configure your online integration. That is outside the bounds of this quickstart guide, although you can read more about what that entails [in the documentation here](https://chronon.ai/Getting_Started.html#online-modes).**
<!-- Ah, I see the note now, This is also something I'd like to contribute if we get to do the Docker quickstart, if not we can just have different guides like the examples I set above to frame that incorporating Chronon Components to the platform can be incremental of sorts. -->

## Conclusion

Using chronon for your feature engineering work simplifies and improves your ML Workflow in a number of ways:

1. You can define features in one place, and use those definitions bot for training data backfills and for online serving.
2. Backfills are automatically point-in-time correct, which avoids label leakage and inconsistencies between training data and online inference. <!-- by default rather than automatically. We do support SNAPSHOT accuracy if that's what people are into -->
3. (not covered in quickstart demo, requires further integration) Orchestration for batchbatch and  <!-- ???? -->
4. (not covered in quickstart demo, requires further integration)  <!-- ?????  Add placeholder if needed--> 


For a more detailed view into the benefits of using Chronon, see [Benefits of Chronon section](#benefits-of-chronon-over-other-approaches) below. <!-- Ah I mentioned this earlier but I see it's right here -->

# Benefits of Chronon over other approaches

Chronon offers the most value to AI/ML practitioners who are trying to build "online" models that are serving requests in real-time as opposed to batch workflows.

Without Chronon, engineers working on these projects need to figure out how to get data to their models for training/eval as well as production inference. As the complexity of data going into these models increases (multiple sources, complex transformation such as windowed aggregations, etc), so does the infrastructure challenge of supporting this data plumbing. <!-- Talk about derived data, contextual, service calls and chaining! That complicates even further if you do adhoc, if we are going to scare them, let's really scare them. -->

Generally, we observed ML practitioners taking one of two approaches:

## The log-and-wait approach

With this approach, users start with the data that is available in the online serving environment from which the model inference will run. Log relevant features to the data warehouse. Once enough data has accumulated, train the model on the logs, and serve with the same data. <!-- rather than users I'd say ML practitioners? -->

Pros:
- Features used to train the model are guaranteed to be available at serving time
- The model can access service call features 
- The model can access data from the the request context


Cons:
- It might take a long to accumulate enough data to train the model
- Performing windowed aggregations is not always possible (running large range queries against production databases doesn't scale, same for event streams)
- Cannot utilize the wealth of data already in the data warehouse
- Maintaining data transformation logic in the application layer is messy

## The replicate offline-online approach

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

<!-- I liked this segment but I think it's a bit reactive in terms of Chronon fixes a) and b), rather than that I'd phrase it:
The challenges in generating ML data involve:
* Availability of online data
* Latency in computation of aggregations.
* Consistency with offline counterparts.
* Data Leakage.
* Multiple apis to fetch data from multiple sources.
* Coupling between feature definition and feature computation.
* Repeated expensive computations of features for different models (thinking of ODM here).
* Orchestration of feature computations (Thinking of chaining here)

Basically what I'm trying to achieve is a "How does chronon help you sleep better at night, when you use it to compute your features"
--> 

# Contributing

We welcome contributions to the Chronon project! Please read our (CONTRIBUTING.md)[CONTRIBUTING.md] for details.

# Support

Use the GitHub issue tracker for reporting bugs or feature requests.
Join our community [Slack Channel](https://chrononworkspace.slack.com/archives/C04K2SLPZU7) for discussions, tips, and support.
