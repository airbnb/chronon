package ai.chronon.spark.test

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{PartitionSpec, TimeUnit, Window}
import org.junit.Test

class HourlyPartitionSpecTest {
  val partitionSpec = PartitionSpec(format = "yyyyMMddHH", spanMillis = WindowUtils.Hour.millis)

  @Test
  def testToAndFromMillis(): Unit = {
    val ts = 1704216057000L // Tuesday, January 2, 2024 17:20:57 UTC => 2024010217
    assert(partitionSpec.at(ts) == "2024010217")
    assert(partitionSpec.epochMillis("2024010217") == 1704214800000L)
  }

  @Test
  def testPartitionSpecMinus(): Unit = {
    val ts = 1704216057000L // Tuesday, January 2, 2024 17:20:57 UTC => 2024010217
    val today = partitionSpec.at(ts)
    assert(partitionSpec.minus(today, new Window(1, TimeUnit.DAYS)) == "2024010117")
    assert(partitionSpec.minus(today, new Window(1, TimeUnit.HOURS)) == "2024010216")
    assert(partitionSpec.shift(today, 1) == "2024010218")
    assert(partitionSpec.shift(today, -3) == "2024010214")
  }

}
