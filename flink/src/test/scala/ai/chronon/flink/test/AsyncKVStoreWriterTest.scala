package ai.chronon.flink.test

import ai.chronon.flink.AsyncKVStoreWriter
import ai.chronon.online.{Api, KVStore}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, DataStreamUtils, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.junit.Test
import org.scalatestplus.mockito.MockitoSugar.mock

class AsyncKVStoreWriterTest {

  val parallelism = 4
  val eventTs = 1519862400075L

  def createExecutionEnvironment(): StreamExecutionEnvironment = {
    val config = new Configuration()
    config.setString("taskmanager.memory.segment-size", "4096")
    // the withUnorderedWaits operation is now using slotSharingGroup
    // total number task slots = sum(max(# of parallelism for each operation))
    config.setString("taskmanager.numberOfTaskSlots", "8")
    StreamExecutionEnvironment.createLocalEnvironment(
      parallelism,
      config
    )
  }

  def createKVRequest(key: String, value: String, dataset: String, ts: Long): PutRequest =
    PutRequest(key.getBytes, value.getBytes, dataset, Some(ts))

  @Test
  def testAsyncWriterSuccessWrites(): Unit = {
    val env = createExecutionEnvironment()
    val source: DataStream[PutRequest] = env
      .fromCollection(
        Range(0, 5).map(i => createKVRequest(i.toString, "test", "my_dataset", eventTs))
      )

    val mockApi = mock[Api]
    val withRetries =
      AsyncKVStoreWriter
        .withUnorderedWaits(
          source,
          new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFG"),
          "testFG"
        )
    val result = new DataStreamUtils(withRetries).collect.toSeq
    assert(result.nonEmpty, "Expect result set to be non-empty")
    assert(result.forall(_.contains(eventTs)))
  }

  @Test
  def testAsyncWriterHandlesPoisonPillWrites(): Unit = {
    val env = createExecutionEnvironment()
    val source: DataStream[KVStore.PutRequest] = env
      .fromCollection(
        Range(0, 5).map(i => createKVRequest(i.toString, "test", "my_dataset", eventTs))
      )

    val mockApi = mock[Api]
    val withRetries =
      AsyncKVStoreWriter
        .withUnorderedWaits(
          source,
          new MockAsyncKVStoreWriter(Seq(false), mockApi, "testFG"),
          "testFG"
        )
    val result = new DataStreamUtils(withRetries).collect.toSeq
    assert(result.nonEmpty, "Expect result set to be non-empty")
    assert(result.forall(_.contains(eventTs)))
  }
}
