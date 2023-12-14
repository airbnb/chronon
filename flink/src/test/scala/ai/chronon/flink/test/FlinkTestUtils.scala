package ai.chronon.flink.test

import ai.chronon.api.{Accuracy, Builders, GroupBy, Operation, TimeUnit, Window}
import ai.chronon.flink.{AsyncKVStoreWriter, FlinkSource, WriteResponse}
import ai.chronon.online.{Api, KVStore}
import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{GroupByServingInfo, PartitionSpec}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.GroupByServingInfoParsed
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.types.StructType
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{when, withSettings}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.Duration
import java.util
import java.util.Collections
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.asScalaBufferConverter

case class E2ETestEvent(id: String, int_val: Int, double_val: Double, created: Long)

class E2EEventSource(mockEvents: Seq[E2ETestEvent]) extends FlinkSource[E2ETestEvent] {
  override def getDataStream(topic: String, groupName: String)(env: StreamExecutionEnvironment,
                                                               parallelism: Int): DataStream[E2ETestEvent] = {
    env.fromCollection(mockEvents)
  }
}

class WatermarkedE2EEventSource(mockEvents: Seq[E2ETestEvent]) extends FlinkSource[E2ETestEvent] {
  def watermarkStrategy: WatermarkStrategy[E2ETestEvent] =
    WatermarkStrategy
      .forBoundedOutOfOrderness[E2ETestEvent](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[E2ETestEvent] {
        override def extractTimestamp(event: E2ETestEvent, previousElementTimestamp: Long): Long =
          event.created
      })
  override def getDataStream(topic: String, groupName: String)(env: StreamExecutionEnvironment,
                                                               parallelism: Int): DataStream[E2ETestEvent] = {
    env.fromCollection(mockEvents).assignTimestampsAndWatermarks(watermarkStrategy)
  }
}

class MockAsyncKVStoreWriter(mockResults: Seq[Boolean], onlineImpl: Api, featureGroup: String)
    extends AsyncKVStoreWriter(onlineImpl, featureGroup) {
  override def getKVStore: KVStore = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    val mockKvStore = mock[KVStore](withSettings().serializable())
    when(mockKvStore.multiPut(ArgumentMatchers.any[Seq[KVStore.PutRequest]]))
      .thenReturn(Future(mockResults))
    mockKvStore
  }
}

class CollectSink extends SinkFunction[WriteResponse] {
  override def invoke(value: WriteResponse, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
  // must be static
  val values: util.List[WriteResponse] = Collections.synchronizedList(new util.ArrayList())
}

object FlinkTestUtils {
  def makeTestGroupByServingInfoParsed(groupBy: GroupBy,
                                       inputSchema: StructType,
                                       outputSchema: StructType): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)

    // Set input avro schema for groupByServingInfo
    groupByServingInfo.setInputAvroSchema(
      inputSchema.toAvroSchema("Input").toString(true)
    )

    // Set key avro schema for groupByServingInfo
    groupByServingInfo.setKeyAvroSchema(
      StructType(
        groupBy.keyColumns.asScala.map { keyCol =>
          val keyColStructType = outputSchema.fields.find(field => field.name == keyCol)
          keyColStructType match {
            case Some(col) => col
            case None =>
              throw new IllegalArgumentException(s"Missing key col from output schema: $keyCol")
          }
        }
      ).toAvroSchema("Key")
        .toString(true)
    )

    // Set value avro schema for groupByServingInfo
    val aggInputColNames = groupBy.aggregations.asScala.map(_.inputColumn).toList
    groupByServingInfo.setSelectedAvroSchema(
      StructType(outputSchema.fields.filter(field => aggInputColNames.contains(field.name)))
        .toAvroSchema("Value")
        .toString(true)
    )
    new GroupByServingInfoParsed(
      groupByServingInfo,
      PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
    )
  }
  def makeGroupBy(keyColumns: Seq[String], filters: Seq[String] = Seq.empty): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val"
            ),
            wheres = filters,
            timeColumn = "created",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = keyColumns,
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        )
      ),
      metaData = Builders.MetaData(
        name = "e2e-count"
      ),
      accuracy = Accuracy.TEMPORAL
    )
}
