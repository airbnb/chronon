package ai.chronon.flink.test

import ai.chronon.flink.window.{TimestampedIR, TimestampedTile}
import ai.chronon.flink.{FlinkJob, SparkExpressionEvalFn}
import ai.chronon.online.{Api, GroupByServingInfoParsed}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}
import org.mockito.Mockito.withSettings
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.jdk.CollectionConverters.asScalaBufferConverter

class FlinkJobIntegrationTest {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build)

  // Decode a PutRequest into a TimestampedTile
  def avroConvertPutRequestToTimestampedTile[T](
      in: PutRequest,
      groupByServingInfoParsed: GroupByServingInfoParsed
  ): TimestampedTile = {
    // Decode the key bytes into a GenericRecord
    val tileBytes = in.valueBytes
    val record = groupByServingInfoParsed.keyCodec.decode(in.keyBytes)

    // Get all keys we expect to be in the GenericRecord
    val decodedKeys: List[String] =
      groupByServingInfoParsed.groupBy.keyColumns.asScala.map(record.get(_).toString).toList

    val tsMills = in.tsMillis.get
    TimestampedTile(decodedKeys, tileBytes, tsMills)
  }

  // Decode a TimestampedTile into a TimestampedIR
  def avroConvertTimestampedTileToTimestampedIR(timestampedTile: TimestampedTile,
                                                groupByServingInfoParsed: GroupByServingInfoParsed): TimestampedIR = {
    val tileIR = groupByServingInfoParsed.tiledCodec.decodeTileIr(timestampedTile.tileBytes)
    TimestampedIR(tileIR._1, Some(timestampedTile.latestTsMillis))
  }

  @Before
  def setup(): Unit = {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  @After
  def teardown(): Unit = {
    flinkCluster.after()
    CollectSink.values.clear()
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

    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFlinkJobEndToEndFG")
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

    // Create some test events with multiple different ids so we can check if tiling/pre-aggregation works correctly
    // for each of them.
    val id1Elements = Seq(E2ETestEvent(id = "id1", int_val = 1, double_val = 1.5, created = 1L),
                          E2ETestEvent(id = "id1", int_val = 1, double_val = 2.5, created = 2L))
    val id2Elements = Seq(E2ETestEvent(id = "id2", int_val = 1, double_val = 10.0, created = 3L))
    val elements: Seq[E2ETestEvent] = id1Elements ++ id2Elements
    val source = new WatermarkedE2EEventSource(elements)

    // Make a GroupBy that SUMs the double_val of the elements.
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))

    // Prepare the Flink Job
    val encoder = Encoders.product[E2ETestEvent]
    val outputSchema = new SparkExpressionEvalFn(encoder, groupBy).getOutputSchema
    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testTiledFlinkJobEndToEndFG")
    val job = new FlinkJob[E2ETestEvent](source, writerFn, groupByServingInfoParsed, encoder, 2)
    job.runTiledGroupByJob(env).addSink(new CollectSink)

    env.execute("TiledFlinkJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.asScala

    // BASIC ASSERTIONS
    // All elements were processed
    assert(writeEventCreatedDS.size == elements.size)
    // check that the timestamps of the written out events match the input events
    // we use a Set as we can have elements out of order given we have multiple tasks
    assertEquals(writeEventCreatedDS.map(_.putRequest.tsMillis).map(_.get).toSet, elements.map(_.created).toSet)
    // check that all the writes were successful
    assertEquals(writeEventCreatedDS.map(_.status), Seq(true, true, true))

    // Assert that the pre-aggregates/tiles are correct
    // Get a list of the final IRs for each key.
    val finalIRsPerKey: Map[List[Any], List[Any]] = writeEventCreatedDS
      .map(writeEvent => {
        // First, we work back from the PutRequest decode it to TimestampedTile and then TimestampedIR
        val timestampedTile =
          avroConvertPutRequestToTimestampedTile(writeEvent.putRequest, groupByServingInfoParsed)
        val timestampedIR = avroConvertTimestampedTileToTimestampedIR(timestampedTile, groupByServingInfoParsed)

        // We're interested in the the keys, Intermediate Result, and the timestamp for each processed event
        (timestampedTile.keys, timestampedIR.ir.toList, writeEvent.putRequest.tsMillis.get)
      })
      .groupBy(_._1) // Group by the keys
      .map((keys) => (keys._1, keys._2.maxBy(_._3)._2)) // pick just the events with largest timestamp

    // Looking back at our test events, we expect the following Intermediate Results to be generated:
    val expectedFinalIRsPerKey = Map(
      List("id1") -> List(4.0), // Add up the double_val of the two 'id1' events
      List("id2") -> List(10.0)
    )

    assertEquals(expectedFinalIRsPerKey, finalIRsPerKey)
  }
}
