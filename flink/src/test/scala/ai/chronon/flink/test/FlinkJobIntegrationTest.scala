package ai.chronon.flink.test

import ai.chronon.flink.{FlinkJob, FlinkSource}
import ai.chronon.online.Api
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}
import org.mockito.Mockito.withSettings
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters.asScalaBufferConverter

class E2EEventSource(mockEvents: Seq[E2ETestEvent]) extends FlinkSource[E2ETestEvent] {
  override def getDataStream(topic: String, groupName: String)(env: StreamExecutionEnvironment, parallelism: Int): DataStream[E2ETestEvent] = {
    env.fromCollection(mockEvents)
  }
}

class CollectSink extends SinkFunction[Option[Long]] {
  override def invoke(value: Option[Long], context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
  // must be static
  val values: util.List[Option[Long]] = Collections.synchronizedList(new util.ArrayList())
}

class FlinkJobIntegrationTest {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(8)
    .setNumberTaskManagers(1)
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

  @Test
  def testFlinkJobEndToEnd(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L),
    )

    val source = new E2EEventSource(elements)
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val encoder = Encoders.product[E2ETestEvent]
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFG")
    val job = new FlinkJob[E2ETestEvent](source, writerFn, groupBy, encoder, 2)

    job.runGroupByJob(env).addSink(new CollectSink)

    env.execute("FlinkJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.asScala
    assert(writeEventCreatedDS.size == elements.size)
    assertEquals(writeEventCreatedDS.map(_.get), elements.map(_.created))
  }
}
