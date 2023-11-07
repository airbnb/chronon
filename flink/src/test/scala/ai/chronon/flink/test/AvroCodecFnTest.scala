package ai.chronon.flink.test

import ai.chronon.flink.{AvroCodecFn, SparkExpressionEvalFn}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType
import org.junit.Test
import org.junit.Assert.assertEquals
import org.scalatest.Assertions.intercept

class AvroCodecFnTest {

  @Test
  def testAvroCodecMaintainsOrder(): Unit = {
    val encoder = Encoders.product[E2ETestEvent]

    val testGroupBy1 = FlinkTestUtils.makeGroupBy(Seq("int_val", "id"))
    val sparkExprEval1 = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      testGroupBy1
    )
    val tiledAvroCodecFn1 =
      AvroCodecFn[E2ETestEvent](
        testGroupBy1,
        encoder.schema,
        sparkExprEval1.getOutputSchema
      )

    // Assert that the key fields are in the order provided by the user
    val keyFields1 = tiledAvroCodecFn1.groupByServingInfoParsed.keyChrononSchema.fields
    assertEquals(Seq("int_val", "id"), keyFields1.map(_.name).toSeq)

    // Construct and assert on a groupby with a different ordering
    val testGroupBy2 = FlinkTestUtils.makeGroupBy(Seq("id", "int_val"))
    val sparkExprEval2 = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      testGroupBy2
    )
    val tiledAvroCodecFn2 =
      AvroCodecFn[E2ETestEvent](
        testGroupBy2,
        encoder.schema,
        sparkExprEval2.getOutputSchema
      )

    // Assert that the key fields are in the order provided by the user
    val keyFields2 = tiledAvroCodecFn2.groupByServingInfoParsed.keyChrononSchema.fields
    assertEquals(Seq("id", "int_val"), keyFields2.map(_.name).toSeq)
  }

  @Test
  def testAvroCodecFailsOnMissingKey(): Unit = {
    val testGroupBy = FlinkTestUtils.makeGroupBy(Seq("id", "int_val"))

    val encoder = Encoders.product[E2ETestEvent]
    val sparkExprEval = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      testGroupBy
    )

    // Remove `id` from the output schema to ensure an error is thrown.
    val modifiedOutputSchema =
      StructType(sparkExprEval.getOutputSchema.fields.filterNot(f => f.name == "id"))

    val exc = intercept[IllegalArgumentException] {
      AvroCodecFn[E2ETestEvent](
        testGroupBy,
        encoder.schema,
        modifiedOutputSchema
      )
    }
    assertEquals("Missing key col from output schema: id", exc.getMessage)
  }
}
