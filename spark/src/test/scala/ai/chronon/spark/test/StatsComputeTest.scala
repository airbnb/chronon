package ai.chronon.spark.test
import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Test
import ai.chronon.spark.stats.StatsGenerator
import com.google.gson.Gson

class StatsComputeTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("StatsComputeTest", local = true)

  private val tableUtils = TableUtils(spark)
  import spark.implicits._

  @Test
  def basicTest(): Unit = {
    val data = Seq(
      (1, Some(1), 1.0, "a"),
      (1, Some(2), 2.0, "b"),
      (2, Some(2), 2.0, "c"),
      (3, None, 1.0, "d")
    )
    val columns = Seq("keyId", "value", "double_value", "string_value")
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns:_*)
    df.show()
    val result = StatsGenerator.summary(df, Seq("keyId"), spark)
    println(result)
    val gson = new Gson()
    println(gson.toJson(result))
    //result.show()
  }

}
