package ai.chronon.flink.test

import ai.chronon.api.{Accuracy, Builders, GroupBy, Operation, TimeUnit, Window}
import ai.chronon.flink.AsyncKVStoreWriter
import ai.chronon.online.{Api, KVStore}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{when, withSettings}
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

case class E2ETestEvent(id: String, int_val: Int, double_val: Double, created: Long)

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

object FlinkTestUtils {

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
