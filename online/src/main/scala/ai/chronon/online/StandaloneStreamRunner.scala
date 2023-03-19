package ai.chronon.online

import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{ForeachWriter, Row, SQLContext, SparkSession}

import java.util.concurrent.ArrayBlockingQueue

object StandaloneStreamingFunctionRunner {

  val queue = new ArrayBlockingQueue[Row](10)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession
      .builder()
      .appName("StreamingDataFrameExample")
      .master("local[1]")
      .getOrCreate()

    //    spark.conf.set("spark.sql.shuffle.partitions", 1)
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._
    val stream = ContinuousMemoryStream.singlePartition[(Int, String, Double)]

    val transformedStream = stream.toDF().selectExpr("_1 + 1 as F1", "_2 as Name")
    val query = transformedStream.writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Continuous("1 second"))
      .foreach(new ForeachWriter[Row] {

        def open(partitionId: Long, version: Long): Boolean = true

        def process(record: Row): Unit = {
          // Write string to connection
          queue.add(record)
        }

        def close(errorOrNull: Throwable): Unit = {}
      })
      .start()

    for (i <- 0 until 100000) {
      val startTime = System.nanoTime()
      stream.addData((i, "Alice", 1.0))
      //      query.processAllAvailable()
      val result = queue.take()
      val endTime = System.nanoTime()
      assert(result.get(0) == i + 1)
      if (i % 10000 == 0) {
        println("i " + i + "delta:" + (endTime - startTime) / 1e6)
      }
    }

    // Wait for the query to terminate
    query.awaitTermination()
  }
}
