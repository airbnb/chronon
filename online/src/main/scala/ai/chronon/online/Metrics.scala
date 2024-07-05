/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.api.Extensions._
import ai.chronon.api._
import com.timgroup.statsd.{Event, NonBlockingStatsDClient, NonBlockingStatsDClientBuilder}

import scala.util.ScalaJavaConversions.ListOps

object Metrics {
  object Environment extends Enumeration {
    type Environment = String
    val MetaDataFetching = "metadata.fetch"
    val JoinFetching = "join.fetch"
    val GroupByFetching = "group_by.fetch"
    val GroupByUpload = "group_by.upload"
    val GroupByStreaming = "group_by.streaming"
    val Fetcher = "fetcher"
    val JoinOffline = "join.offline"
    val GroupByOffline = "group_by.offline"
    val StagingQueryOffline = "staging_query.offline"

    val JoinLogFlatten = "join.log_flatten"
    val LabelJoin = "label_join"
  }

  import Environment._

  object Tag {
    val GroupBy = "group_by"
    val Join = "join"
    val JoinPartPrefix = "join_part_prefix"
    val StagingQuery = "staging_query"
    val Environment = "environment"
    val Production = "production"
    val Accuracy = "accuracy"
    val Team = "team"
  }

  object Name {
    val FreshnessMillis = "freshness.millis"
    val FreshnessMinutes = "freshness.minutes"
    val LatencyMillis = "latency.millis"
    val LagMillis: String = "lag.millis"
    val BatchLagMillis: String = "micro_batch_lag.millis"
    val QueryDelaySleepMillis: String = "chain.query_delay_sleep.millis"
    val LatencyMinutes = "latency.minutes"

    val PartitionCount = "partition.count"
    val RowCount = "row.count"
    val RequestCount = "request.count"
    val ColumnBeforeCount = "column.before.count"
    val ColumnAfterCount = "column.after.count"
    val FailureCount = "failure.ratio"

    val Bytes = "bytes"
    val KeyBytes = "key.bytes"
    val ValueBytes = "value.bytes"
    val FetchExceptions = "fetch.exception_count"
    val FetchNulls = "fetch.null_count"
    val FetchCount = "fetch.count"

    val PutKeyNullPercent = "put.key.null_percent"
    val PutValueNullPercent = "put.value.null_percent"

    val Exception = "exception"
    val validationFailure = "validation.failure"
    val validationSuccess = "validation.success"
  }

  object Context {
    val sampleRate: Double = 0.1

    def apply(environment: Environment, join: Join): Context = {
      Context(
        environment = environment,
        join = join.metaData.cleanName,
        production = join.metaData.isProduction,
        team = join.metaData.owningTeam
      )
    }

    def apply(environment: Environment, groupBy: GroupBy): Context = {
      Context(
        environment = environment,
        groupBy = groupBy.metaData.cleanName,
        production = groupBy.metaData.isProduction,
        accuracy = groupBy.inferredAccuracy,
        team = groupBy.metaData.owningTeam,
        join = groupBy.sources.toScala
          .find(_.isSetJoinSource)
          .map(_.getJoinSource.join.metaData.cleanName)
          .orNull
      )
    }

    def apply(joinContext: Context, joinPart: JoinPart): Context = {
      joinContext.copy(groupBy = joinPart.groupBy.metaData.cleanName,
                       accuracy = joinPart.groupBy.inferredAccuracy,
                       joinPartPrefix = joinPart.prefix)
    }

    def apply(environment: Environment, stagingQuery: StagingQuery): Context = {
      Context(
        environment = environment,
        groupBy = stagingQuery.metaData.cleanName,
        production = stagingQuery.metaData.isProduction,
        team = stagingQuery.metaData.owningTeam
      )
    }

    val statsPort: Int = System.getProperty("ai.chronon.metrics.port", "8125").toInt
    val tagCache: TTLCache[Context, String] = new TTLCache[Context, String](
      { ctx => ctx.toTags.reverse.mkString(",") },
      { ctx => ctx },
      ttlMillis = 5 * 24 * 60 * 60 * 1000 // 5 days
    )

    private val statsClient: NonBlockingStatsDClient =
      new NonBlockingStatsDClientBuilder().prefix("ai.zipline").hostname("localhost").port(statsPort).build()

  }

  case class Context(environment: Environment,
                     join: String = null,
                     groupBy: String = null,
                     stagingQuery: String = null,
                     production: Boolean = false,
                     accuracy: Accuracy = null,
                     team: String = null,
                     joinPartPrefix: String = null,
                     suffix: String = null)
      extends Serializable {

    def withSuffix(suffixN: String): Context = copy(suffix = (Option(suffix) ++ Seq(suffixN)).mkString("."))

    // Tagging happens to be the most expensive part(~40%) of reporting stats.
    // And reporting stats is about 30% of overall fetching latency.
    // So we do array packing directly instead of regular string interpolation.
    // This simply creates "key:value"
    // The optimization shaves about 2ms of 6ms of e2e overhead for 500 batch size.
    def buildTag(key: String, value: String): String = {
      val charBuf = new Array[Char](key.length + value.length + 1)
      key.getChars(0, key.length, charBuf, 0)
      value.getChars(0, value.length, charBuf, key.length + 1)
      charBuf.update(key.length, ':')
      new String(charBuf)
    }

    private lazy val tags = Metrics.Context.tagCache(this)
    private val prefixString = environment + Option(suffix).map("." + _).getOrElse("")

    private def prefix(s: String): String =
      new java.lang.StringBuilder(prefixString.length + s.length + 1)
        .append(prefixString)
        .append('.')
        .append(s)
        .toString

    @transient private lazy val stats: NonBlockingStatsDClient = Metrics.Context.statsClient

    def increment(metric: String): Unit = stats.increment(prefix(metric), tags)

    def incrementException(exception: Throwable)(implicit logger: org.slf4j.Logger): Unit = {
      val stackTrace = exception.getStackTrace
      val exceptionSignature = if (stackTrace.isEmpty) {
        exception.getClass.toString
      } else {
        val stackRoot = stackTrace.apply(0)
        val file = stackRoot.getFileName
        val line = stackRoot.getLineNumber
        val method = stackRoot.getMethodName
        s"[$method@$file:$line]${exception.getClass.toString}"
      }
      logger.error(s"Exception Message: ${exception.traceString}")
      stats.increment(prefix(Name.Exception), s"$tags,${Metrics.Name.Exception}:${exceptionSignature}")
    }

    def distribution(metric: String, value: Long): Unit =
      stats.distribution(prefix(metric), value, Context.sampleRate, tags)

    def count(metric: String, value: Long): Unit = stats.count(prefix(metric), value, tags)

    def gauge(metric: String, value: Long): Unit = stats.gauge(prefix(metric), value, tags)

    def gauge(metric: String, value: Double): Unit = stats.gauge(prefix(metric), value, tags)

    def recordEvent(metric: String, event: Event): Unit = stats.recordEvent(event, prefix(metric), tags)

    def toTags: Array[String] = {
      val joinNames: Array[String] = Option(join).map(_.split(",")).getOrElse(Array.empty[String]).map(_.sanitize)
      assert(
        environment != null,
        "Environment needs to be set - group_by.upload, group_by.streaming, join.fetching, group_by.fetching, group_by.offline etc")
      val buffer = new Array[String](7 + joinNames.length)
      var counter = 0

      def addTag(key: String, value: String): Unit = {
        if (value == null) return
        assert(counter < buffer.length, "array overflow")
        buffer.update(counter, buildTag(key, value))
        counter += 1
      }

      joinNames.foreach(addTag(Tag.Join, _))

      val groupByName = Option(groupBy).map(_.sanitize)
      groupByName.foreach(addTag(Tag.GroupBy, _))

      addTag(Tag.StagingQuery, stagingQuery)
      addTag(Tag.Production, production.toString)
      addTag(Tag.Team, team)
      addTag(Tag.Environment, environment)
      addTag(Tag.JoinPartPrefix, joinPartPrefix)
      addTag(Tag.Accuracy, if (accuracy != null) accuracy.name() else null)
      buffer
    }
  }
}
