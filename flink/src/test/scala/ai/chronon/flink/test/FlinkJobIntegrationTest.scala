package ai.chronon.flink.test

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{GroupBy, GroupByServingInfo, PartitionSpec}
import ai.chronon.flink.{FlinkJob, FlinkSource, SparkExpressionEvalFn, WriteResponse}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.{Api, GroupByServingInfoParsed}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}
import org.mockito.Mockito.withSettings
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters.asScalaBufferConverter

class E2EEventSource(mockEvents: Seq[E2ETestEvent]) extends FlinkSource[E2ETestEvent] {
  override def getDataStream(topic: String, groupName: String)(env: StreamExecutionEnvironment,
                                                               parallelism: Int): DataStream[E2ETestEvent] = {
    env.fromCollection(mockEvents)
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

class FlinkJobIntegrationTest {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(2)
      .build)

  @Before
  def setup(): Unit = {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  @After
  def teardown(): Unit = {
    flinkCluster.after()
  }

  private def makeTestGroupByServingInfoParsed(groupBy: GroupBy,
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

  @Test
  def testFlinkJobEndToEnd(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val source = new E2EEventSource(elements)
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val encoder = Encoders.product[E2ETestEvent]

    val outputSchema = new SparkExpressionEvalFn(encoder, groupBy).getOutputSchema

    val groupByServingInfoParsed = makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFG")
    val job = new FlinkJob[E2ETestEvent](source, writerFn, groupByServingInfoParsed, encoder, 2)

    job.runGroupByJob(env).addSink(new CollectSink)

    env.execute("FlinkJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.asScala

    assert(writeEventCreatedDS.size == elements.size)
    // check that the timestamps of the written out events match the input events
    // we use a Set as we can have elements out of order given we have multiple tasks
    assertEquals(writeEventCreatedDS.map(_.putRequest.tsMillis).map(_.get).toSet, elements.map(_.created).toSet)
    // check that all the writes were successful
    assertEquals(writeEventCreatedDS.map(_.status), Seq(true, true, true))
  }

  @Test
  def testTiledFlinkJobEndToEnd(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1L),
      E2ETestEvent("test2", 13, 1.6, 2L),
      E2ETestEvent("test3", 14, 1.7, 3L)
    )

    val source = new E2EEventSource(elements)
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val encoder = Encoders.product[E2ETestEvent]

    val outputSchema = new SparkExpressionEvalFn(encoder, groupBy).getOutputSchema

    val groupByServingInfoParsed = makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFG")
    val job = new FlinkJob[E2ETestEvent](source, writerFn, groupByServingInfoParsed, encoder, 2)

    job.runTiledGroupByJob(env).addSink(new CollectSink)

    env.execute("FlinkJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.asScala

    assert(writeEventCreatedDS.size == elements.size)
    // check that the timestamps of the written out events match the input events
    // we use a Set as we can have elements out of order given we have multiple tasks
    assertEquals(writeEventCreatedDS.map(_.putRequest.tsMillis).map(_.get).toSet, elements.map(_.created).toSet)
    // check that all the writes were successful
    assertEquals(writeEventCreatedDS.map(_.status), Seq(true, true, true))
  }
}
