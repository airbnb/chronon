package ai.chronon.spark.test

import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.spark.stats.CompareJob
import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.junit.Test

class CompareTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("CompareTest", local = true)

  private val tableUtils = TableUtils(spark)

  def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

  @Test
  def basicTest(): Unit = {
    val leftData = Seq(
      (1, Some(1), 1.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10"),
      (1, Some(2), 2.0, "b", toTs("2021-04-10 11:00:00"), "2021-04-10"),
      (2, Some(2), 2.0, "c", toTs("2021-04-10 15:00:00"), "2021-04-10"),
      (3, None, 1.0, "d", toTs("2021-04-10 19:00:00"), "2021-04-10")
    )

    val rightData = Seq(
      (1, Some(1), 5.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10"),
      (2, Some(3), 2.0, "b", toTs("2021-04-10 11:00:00"), "2021-04-10"),
      (2, Some(5), 6.0, "c", toTs("2021-04-10 15:00:00"), "2021-04-10"),
      (3, None, 1.0, "d", toTs("2021-04-10 19:00:00"), "2021-04-10")
    )

    val columns = Seq("serial", "value", "rating", "keyId", "ts", "ds")
    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(columns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(columns:_*)

    leftDf.show()
    rightDf.show()

    val result = CompareJob.compare(leftDf, rightDf, Seq("keyId", "ts", "ds"))
    println(result)
    val gson = new Gson()
    println(gson.toJson(result))
    //result.show()
  }

}
