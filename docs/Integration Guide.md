# Intro

Chronon can automatically generate spark pipelines from user configuration.

Chronon utilizes Hive and Kafka as data sources, but since we rely on Spark, it should be fairly easy to incorporate any spark supported data source.

We recommend integrating with airflow to schedule these pipelines. But any scheduler should be straight forward to integrate with.


![Architecture](./images/Overall%20Architecture.png)

## Pre-requisites


## Integrations

There are essentially four integration points:

- Chronon Repository - This is where your users will define chronon configurations. We recommend that this live within a airflow repository, or your own scheduler's repository to make deployment easy. Once you have the repository setup you can begin using chronon for offline batch pipelines.

- For online Serving
  - KV Store - for storing and serving features in low latency. This can be any kv store that can support point write, point lookup, scan and bulk write.
  - Event decoding - for reading bytes from kafka and converting them into a Chronon Event or a Chronon Mutation. If you have a convention between how you convert data in kafka into data in warehouse, you would need to follow that same convention to decode as well.

- Airflow - for scheduling spark pipelines that backfill training.

## Online API

You can simply drop setup the repo and begin using chronon
### KV Store API

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

### Stream Decoder API
- If the kafka topic contains a simple event you can simply set before to null.
- If event is a db table insert - before is null, after contains value.
- If event is a table delete - after is null, before contains value.
- If event is an update - both before and after should be present.

```scala

case class Mutation(schema: StructType = null, before: Array[Any] = null, after: Array[Any] = null)

abstract class StreamDecoder extends Serializable {
  def decode(bytes: Array[Byte]): Mutation
  def schema: StructType
}
```

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


Putting stream decoder and kv store together, you need to implement the abstract class below.

```scala
abstract class Api(userConf: Map[String, String]) extends Serializable {
  def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder
  def genKvStore: KVStore
}
```

`userConf` is captured from commandline arguments to the `run.py` script or to the `chronon-uber-jar` with `ai.chronon.spark.Driver` as the main class `-Zkey1=value1 -Zkey2=value2` becomes `{key1: value1, key2: value2}` initializer argument to the Api class. You can use that to set KVStore params, or kafka params for streaming jobs or bulk upload jobs.

### Repository Setup

mkdir

```scala

```



