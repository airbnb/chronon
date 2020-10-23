package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.api.Config.Constants
import junit.framework.TestCase

class JoinTest extends TestCase {
  val spark = SparkSessionBuilder.build("JoinTest")
  def testEntities: Unit = {
    val eventSchema = List(
      DataGen.Column("user", StringType, 10),
      DataGen.Column(Constants.TimeColumn, LongType, 180),
      DataGen.Column("session_length", IntType, 10000)
    )

    val eventDf = DataGen.events(spark, eventSchema, 1000000)

  }
}
