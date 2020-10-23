package ai.zipline.spark.test

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build(name: String): SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .appName(name)
      .getOrCreate()
}
