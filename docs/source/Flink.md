
# Chronon on Flink

_**Important**: The Flink connector is an experimental feature that is still in the process of being open-sourced._

Chronon on Flink is an alternative to Chronon on Spark Streaming. It's intended for organizations that don't have access to Spark Streaming or want to use the [The Tiled Architecture](./Tiled_Architecture.md).

## How to use the Flink

The process of integrating Flink will be different at each organization. The overall idea is simple: you need to integrate the [FlinkJob](https://github.com/airbnb/chronon/blob/master/flink/src/main/scala/ai/chronon/flink/FlinkJob.scala) so that it reads an event stream (e.g. Kafka) and writes out to a KV store.

There are two versions of the Flink app that you can choose from, tiled and untiled. See [The Tiled Architecture](./Tiled_Architecture.md) for an overview of the differences. In short, the untiled version writes out events to the KV store, whereas the tiled version writes out pre-aggregates. In [FlinkJob.scala](https://github.com/airbnb/chronon/blob/master/flink/src/main/scala/ai/chronon/flink/FlinkJob.scala) you will find both options.

You will also likely need to modify your `KVStore` implementation while integrating Flink. 

## Overview of the Flink operators

The operators for the tiled and untiled Flink jobs differ slightly. The main difference is that the tiled job is stateful and contains a window operator. This section goes over the tiled version. See [FlinkJob.scala](https://github.com/airbnb/chronon/blob/master/flink/src/main/scala/ai/chronon/flink/FlinkJob.scala) for details on the untiled version.

### The tiled Flink job

The Flink job contains five main operators
1. Source - Reads events of type `T` from a source, which is often a Kafka topic. The generic type `T` could be a POJO, Scala case class, [Thrift](https://thrift.apache.org/), [Proto](https://protobuf.dev/), etc.
2. Spark expression evaluation - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input data. This operator runs Spark inside the Flink app using CatalystUtil.
3. Window/tiling - This is the main tiling operator. It uses a window to aggregate incoming events and keep track of the IRs. It outputs the pre-aggregates on every event so they are written out to the KV store and the fetcher has access to fresh values.
4. Avro conversion - Finishes [Avro-converting](https://avro.apache.org/) the output of the window (the IRs) to a form that can be written out to the KV store (`PutRequest` object).
5. KV store sink - Writes the `PutRequest` objects to the KV store using the `AsyncDataStream` API.

### End-to-end example

This example shows a **simplified version** of what happens to events as they move through the Flink operators.

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


#### 1. Source

The Source operator consumes from Kafka and deserializes the events into typed objects. For our Ice Cream shop example, we have this Proto:

```Scala
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
    ice_cream_cone_size =  "large" // Not used in the GroupBy definition 
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

The final operator asynchronously writes the `PutRequests` to the KV store. These tiled are later decoded by the Fetcher and merged to calculate the final feature values.