package ai.chronon.flink.test

import ai.chronon.flink.SparkExpressionEvalFn
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, DataStreamUtils, StreamExecutionEnvironment}
import org.apache.spark.sql.Encoders
import org.junit.Test

class SparkExpressionEvalFnTest {

  @Test
  def testBasicSparkExprEvalSanity(): Unit = {
    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val encoder = Encoders.product[E2ETestEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      groupBy
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[E2ETestEvent] = env.fromCollection(elements)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = new DataStreamUtils(sparkExprEvalDS).collect.toSeq
    // lets check the size
    assert(result.size == elements.size, "Expect result sets to include all 3 rows")
    // lets check the id field
    assert(result.map(_.apply("id")).toSet == Set("test1", "test2", "test3"))
  }

}
