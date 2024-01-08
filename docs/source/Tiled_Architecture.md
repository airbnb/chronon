
# Tiled Architecture

**Important**: Tiling is a new feature that is still in the process of being open-sourced.

## What is tiling?

Tiling or the tiled architecture is a modification to Chronon's online architecture to store pre-aggregates (also known as "IRs" or Intermediate Representations) in the Key-Value store instead of individual events. 

The primary purpose of tiling is to increase scalability and decrease feature serving latency. 

The tiled architecture, as it is now, requires [Flink](https://flink.apache.org/).

### Chronon without tiling
The regular, untiled version works as pictured in Figure 1.
- The "write" path: reads an event stream, processes the events in Spark, then writes them out to a datastore. 
- The "read" path: reads O(events) events from the store, aggregates them, and returns the feature values to the user.

![Architecture](../images/Untiled_Architecture.png)
_Figure 1: The untiled architecture_

At scale, aggregating O(n) events each time there is a request can become costly. For example, if you have an event stream producing 10 events/sec for a certain key, a request for a feature with a 12-hour window will have to fetch and aggregate 432,000 events every single time. For a simple GroupBy that counts the number of events for a key, Chronon would iterate over 432,000 items and count the total.

### Chronon with tiling
The tiled architecture, depicted in Figure 2, works differently:
- The "write" path: reads an event stream, processes and pre-aggregates the events in a stateful Flink app, then writes out the pre-aggregates to "tiles" in the store.
- The "read" path: reads O(tiles) tiles from the store, merges the pre-aggregates, and returns the feature values to the user.

![Architecture](../images/Tiled_Architecture.png)
_Figure 2: The tiled architecture_

Tiling shifts a significant part of the aggregation work to the write path. Instead of 

Using the same example as above (an event stream producing 10 events/sec for a certain key, and a GroupBy with a 12-hour window), a request for feature values would fetch and merge 12 or 13 1-hour tiles. For a simple GroupBy that counts the number of events for a key, Chronon would iterate over 13 numbers and add them together. That's significantly less work.

## Should I use tiling?

In general, tiling improves scalability and decreases feature serving latency. Some use cases are:
- You want to decrease feature serving latency
- You don't have access to Spark Streaming
- You don't have access to a datastore with range queries
- You want to reduce fanout to your datastore.

In particular, organizations operating a significant scale should consider using the tiled architecture.

## How to enable tiling

To use tiling, you need Flink to be available in your organization (or modify Chronon to work with other stream processing frameworks).

To migrate to tiling, start with the write path. If you are not yet using Chronon on Flink, you can start by integrating **Flink without tiling** and then enable tiling. In the [FlinkJob](https://github.com/airbnb/chronon/blob/master/flink/src/main/scala/ai/chronon/flink/FlinkJob.scala) class there are two versions of the app, one tiled and one untiled. 

Once you have your Flink app set up and writing tiles to your datastore, it's time to modify the read path. This is simple: add `enable_tiling=true` to the [customJson](https://github.com/airbnb/chronon/blob/48b789dd2c216c62bbf1d74fbf4e779f23db541f/api/py/ai/chronon/group_by.py#L561) of any GroupBy definition.  

## End-to-end tiling example

This example shows a **simplified version** of what happens to events as they move through the Flink operators and are fetched by the Chronon Fetcher.

Say we have a high-tech Ice Cream shop and we want to create an ML model. We want to define features for:
- Counting the number of ice cream cones a person has bought in the last 6 hours.
- Keeping track of the last ice cream flavor a person had in the last 6 hours.

A GroupBy might looks like this:
```python
ice_cream_group_by = GroupBy(
    sources=Source(
        events=ttypes.EventSource(
            query=Query(
                selects=select(
                    customer_id="customer_id",
                    flavor="ice_cream_flavor",
                ),
                time_column="created",
                …
            )
        )
    ),
    keys=["customer_id"],
    aggregations=[
        Aggregation(
            input_column="customer_id",
            operation=Operation.COUNT,
            windows=[Window(length=6, timeUnit=TimeUnit.HOURS)],
        ),
        Aggregation(
            input_column="flavor",
            operation=Operation.LAST,
            windows=[Window(length=6, timeUnit=TimeUnit.HOURS)],
        ),
    ],
    accuracy=Accuracy.TEMPORAL,
    online=True,
    …
)
```

### Flink, the "write path"

The Flink job contains five main operators
 1. Source - Reads events of type `T` from a source, which is often a Kafka topic. The generic type `T` could be a Scala case class, [Thrift](https://thrift.apache.org/), [Proto](https://protobuf.dev/), etc. 
 2. Spark expression evaluation - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input data. This runs Spark inside the Flink app using CatalystUtil.
 3. Window/tiling - This is the main tiling operator. It uses a window to aggregate incoming events and keep track of the IRs. It outputs the pre-aggregates on every event so they are written out to the KV store and the fetcher has access to fresh values.
 4. Avro conversion - Finishes [Avro](https://avro.apache.org/)-converting the output of the window (the IRs) to a form that can be written out to the KV store (PutRequest object).
 5. KV store sink - Writes the `PutRequest` objects to the KV store using the `AsyncDataStream` API.

Counting the number of ice cream cones a person has bought in the last 6 hrs.
Keep track of the last ice cream flavor a person had in the last 6 hrs.

#### 1. Source

The Source operator consumes from Kafka and deserializes the events into typed objects. For our Ice Cream shop example, we have this Proto:

```json
IceCreamEventProto(
     customer_id: _root_.scala.Predef.String = "",
     created: _root_.scala.Long = 0L,
     ice_cream_flavor: _root_.scala.Predef.String = "",
     ice_cream_cone_size: _root_.scala.Predef.String = "",
)
```

#### 2. Spark expression evaluation

Here, the operator takes the object of type `T`, performs the selects and filters defined in the GroupBy, and outputs a `Map[String, Any]`.

```scala
// Input
IceCreamEventProto(
    customer_id = "Alice",
    created = 1000L,
    ice_cream_flavor = "chocolate",
    ice_cream_cone_size =  "large" // Not used in GroupBy definition 
)
// Output
Map(
    "customer_id" -> "Alice",
    "created" -> 1000L,
    "flavor" -> "chocolate"
)
```

#### 3. Window operator

The window operator pre-aggregates the incoming `Map(String -> Any)` and outputs an array of IRs. Example:

Event 1 comes in with `Map("Alice", 1000L, "chocolate")`. 
- Pre-aggregates for key "Alice": `[count: 1, last_flavor: "chocolate"]`

Event 2 comes in for `Map("Bob", 1200L, "strawberry")`
- Pre-aggregates for key "Bob": `[count: 1, last_flavor: "strawberry"]`

Event 3 comes in `Map("Alice", 1500L, "olive oil")`
- Pre-aggregates for key "Alice": `[count: 2, last_flavor: "olive oil"]`

#### 4. Avro conversion

This operator uses Avro to finish encoding the array of IRs into bytes and creates a `PutRequest`.

Input: `[2, "olive oil"]`

Output: `PutRequest(keyBytes: a927dcc=, valueBytes: d823eaa82==, ...)` (varies depending on your specific `KVStore` implementation).

#### 5. KVStore Sink

The final operator writes to the KV store.

### Fetcher, the "read path"

To serve the data Flink stored in the KV store, the fetcher will fetch all the necessary tiles from the KV store(s), decode them using Avro, merge the pre-aggregates, and return the feature values to the user.

In our example, suppose we have two KV stores, one batch and one online.

Fetch IRs the batch KV store and decode it:
```
[-9:00, 00:00) -> [4, "chocolate"]
```
Fetch tiles from the online KV store (these were written by Flink):
```
[0:00, 1:00) -> [1, "chocolate"]
[1:00, 2:00) -> [2, "vanilla"]
[2:00, 3:00) -> [2, "lemon"]
```
Combine everything and finalize: `[9, "lemon"]`. 🎉

