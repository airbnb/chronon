package ai.chronon.spark.test

import ai.chronon.online.Fetcher.{Request, Response}
import ai.chronon.online.{JavaRequest, LoggableResponseBase64}
import ai.chronon.spark.TableUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.ScalaJavaConversions._

object FetcherTestUtil {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  def joinResponses(spark: SparkSession,
                    requests: Array[Request],
                    mockApi: MockApi,
                    useJavaFetcher: Boolean = false,
                    runCount: Int = 1,
                    samplePercent: Double = -1,
                    logToHive: Boolean = false,
                    debug: Boolean = false)(implicit ec: ExecutionContext): (List[Response], DataFrame) = {
    val chunkSize = 100
    @transient lazy val fetcher = mockApi.buildFetcher(debug)
    @transient lazy val javaFetcher = mockApi.buildJavaFetcher()

    def fetchOnce = {
      var latencySum: Long = 0
      var latencyCount = 0
      val blockStart = System.currentTimeMillis()
      val result = requests.iterator
        .grouped(chunkSize)
        .map { oldReqs =>
          // deliberately mis-type a few keys
          val r = oldReqs
            .map(r =>
              r.copy(keys = r.keys.mapValues { v =>
                if (v.isInstanceOf[java.lang.Long]) v.toString else v
              }.toMap))
          val responses = if (useJavaFetcher) {
            // Converting to java request and using the toScalaRequest functionality to test conversion
            val convertedJavaRequests = r.map(new JavaRequest(_)).toJava
            val javaResponse = javaFetcher.fetchJoin(convertedJavaRequests)
            FutureConverters
              .toScala(javaResponse)
              .map(
                _.toScala.map(jres =>
                  Response(
                    Request(jres.request.name, jres.request.keys.toScala.toMap, Option(jres.request.atMillis)),
                    jres.values.toScala.map(_.toScala)
                  )))
          } else {
            fetcher.fetchJoin(r)
          }

          // fix mis-typed keys in the request
          val fixedResponses =
            responses.map(resps => resps.zip(oldReqs).map { case (resp, req) => resp.copy(request = req) })
          System.currentTimeMillis() -> fixedResponses
        }
        .flatMap {
          case (start, future) =>
            val result = Await.result(future, Duration(10000, SECONDS)) // todo: change back to millis
            val latency = System.currentTimeMillis() - start
            latencySum += latency
            latencyCount += 1
            result
        }
        .toList
      val latencyMillis = latencySum.toFloat / latencyCount.toFloat
      val qps = (requests.length * 1000.0) / (System.currentTimeMillis() - blockStart).toFloat
      (latencyMillis, qps, result)
    }

    // to overwhelm the profiler with fetching code path
    // so as to make it prominent in the flamegraph & collect enough stats

    var latencySum = 0.0
    var qpsSum = 0.0
    var loggedValues: Seq[LoggableResponseBase64] = null
    var result: List[Response] = null
    (0 until runCount).foreach { _ =>
      val (latency, qps, resultVal) = fetchOnce
      result = resultVal
      loggedValues = mockApi.flushLoggedValues.toSeq
      latencySum += latency
      qpsSum += qps
    }
    val fetcherNameString = if (useJavaFetcher) "Java" else "Scala"

    logger.info(s"""
                   |Averaging fetching stats for $fetcherNameString Fetcher over ${requests.length} requests $runCount times
                   |with batch size: $chunkSize
                   |average qps: ${qpsSum / runCount}
                   |average latency: ${latencySum / runCount}
                   |""".stripMargin)
    val loggedDf = mockApi.loggedValuesToDf(loggedValues, spark)
    if (logToHive) {
      TableUtils(spark).insertPartitions(
        loggedDf,
        mockApi.logTable,
        partitionColumns = Seq("ds", "name")
      )
    }
    if (samplePercent > 0) {
      logger.info(s"logged count: ${loggedDf.count()}")
      loggedDf.show()
    }
    result -> loggedDf
  }
}
