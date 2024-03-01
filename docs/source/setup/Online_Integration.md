# Online Integration

This document covers how to integrate Chronon with your online KV store, which is the backend that powers low latency serving of individual feature vectors in the online environment.

This integration gives Chronon the ability to:

1. Perform batch uploads of feature values to the KV store
2. Perform streaming updates to those features
3. Fetch features via the `Fetcher` API

## Example

If you'd to start with an example, please refer to the [MongoDB Implementation in the Quickstart Guide](https://github.com/airbnb/chronon/tree/main/quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online). This provides a complete working example of how to integrate Chronon with MongoDB. 

## Components

**KVStore**: The biggest part of the API implementation is the [KVStore](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Api.scala#L43).

```scala
object KVStore {
  // `afterTsMillis` implies that this is a range scan of all values with `timestamp` >= to the specified one. This can be implemented efficiently, if `timestamp` can be a secondary key. Some databases have a native version id concept which also can map to timestamp.
  case class GetRequest(keyBytes: Array[Byte], dataset: String, afterTsMillis: Option[Long] = None)

  // response is a series of values that are 
  case class TimedValue(bytes: Array[Byte], millis: Long)
  case class GetResponse(request: GetRequest, values: Try[Seq[TimedValue]]) {
    def latest: Try[TimedValue] = values.map(_.maxBy(_.millis))
  }

  case class PutRequest(keyBytes: Array[Byte], valueBytes: Array[Byte], dataset: String, tsMillis: Option[Long] = None)
}

trait KVStore {
  def create(dataset: String): Unit

  // Used by the Chronon client to fetch features
  def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]]

  // Used by spark streaming job to write values
  def multiPut(keyValueDatasets: Seq[PutRequest]): Future[Seq[Boolean]]

  // Used by spark upload job to bulk upload data into kv store
  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit
}
```

There are three functions to implement as part of this integration:

1. `create`: which takes a string and creates a new database/dataset with that name.
2. `multiGet`: which takes a `Seq` of [`GetRequest`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Api.scala#L33) and converts them into a `Future[Seq[GetResponse]]` by querying the underlying KVStore.
3. `multiPut`: which takes a `Seq` of [`PutRequest`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Api.scala#L38) and converts them into `Future[Seq[Boolean]]` (success/fail) by attempting to insert them into the underlying KVStore.
4. `bulkPut`: to upload a hive table into your kv store. It takes the table name and partitions as `String`s as well as the dataset as a `String`. If you have another mechanism (like an airflow upload operator) to upload data from hive into your kv stores you don't need to implement this method.

See the [MongoDB example here](https://github.com/airbnb/chronon/blob/main/quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/MongoKvStore.scala).

**StreamDecoder**: This is responsible for "decoding" or converting the raw values that Chronon streaming jobs will read into events that it knows how to process.

```scala
case class Mutation(schema: StructType = null, before: Array[Any] = null, after: Array[Any] = null)

abstract class StreamDecoder extends Serializable {
  def decode(bytes: Array[Byte]): Mutation
  def schema: StructType
}
```

At a high level, there are two types of inputs streams that Chronon might listen to:

1. Events: These are the most common type of streaming data, and can be thought of as the "standard" insert-only kafka log. The key differentiator from Mutations is that Events are immutable, and cannot be updated/deleted (although new events with the same key can be emitted).
2. Mutations: These are streaming updates to specific entities. For example, an item in an online product catalog might get updates to its price or description, or it might get deleted altogether. Unlike normal events, these can be modeled as `INSERT/UPDATE/DELETE`.

Mutations are usually captured changes from production databases. [Debezium](https://debezium.io/) is one tool that can be used for this if you don't have a Mutation capture system in place already. The benefit of Mutations processing is that you can use production Databases as sources of feature data, without issuing large range queries against those databases. Chronon can keep large windowed aggregations up-to-date by listening to the Mutations log and lookup requests don't add additional load to production DBs.

If you don't have a Mutation capture system and don't wish to set one up, you can still use Chronon streaming with plain Events.

In the API, the `Mutation` is modeled as the general case for both `Events` and `Mutations`, since `Events` can be viewed as a subset of `Mutation`.

The `StreamDecoder` is responsible for two function implementations:

1. `decode`: Which takes in an `Array[Byte]` and converts it to a `Mutation`,
2. `schema`: Which provides the `StructType` for the given `GroupBy`

Chronon has a type system that can map to Spark's or Avro's type system. Schema is based on the below table which contains Java types corresponding to the Chronon schema types. StreamDecoder should produce mutations that comply.

| Chronon Type   |  Java Type            |
|----------------|-----------------------|
| IntType        | java.lang.Integer     |
| LongType       | java.lang.Long        |
| DoubleType     | java.lang.Double      |
| FloatType      | java.lang.Float       |
| ShortType      | java.lang.Short       |
| BooleanType    | java.lang.Boolean     |
| ByteType       | java.lang.Byte        |
| StringType     | java.lang.String      |
| BinaryType     | Array[Byte]           |
| ListType       | java.util.List[Byte]  |
| MapType        | java.util.Map[Byte]   |
| StructType     | Array[Any]            |


See the [Quickstart example here](https://github.com/airbnb/chronon/blob/main/quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/QuickstartMutationDecoder.scala).


**API:** The main API that requires implementation is [API](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Api.scala#L151). This combines the above implementations with other client and logging configuration.

[ChrononMongoOnlineImpl](https://github.com/airbnb/chronon/blob/main/quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/ChrononMongoOnlineImpl.scala) Is an example implemenation of the API.


Once you have the api object you can build a fetcher class using the api object like so
```scala
val api = new MyApiImplementation(myParams)
val fetcher = api.buildFetcher()
//or
val javaFetcher = api.buildJavaFetcher()

// you can use fetcher to begin fetching values (there is a java version too)
fetcher.fetchJoins(Seq(Request(name="my.join.name", keys=Map("user" -> "bob", "item" -> "pizza"))))

// if your date partition column convention in your warehouse differs from "yyyy-MM-dd" you should set a partitionSpec
fetcher.setPartitionSpec("yyyyMMdd")
```

`userConf` is captured from commandline arguments to the `run.py` script or to the `chronon-uber-jar` with `ai.chronon.spark.Driver` as the main class `-Zkey1=value1 -Zkey2=value2` becomes `{key1: value1, key2: value2}` initializer argument to the Api class. You can use that to set KVStore params, or kafka params for streaming jobs or bulk upload jobs.
