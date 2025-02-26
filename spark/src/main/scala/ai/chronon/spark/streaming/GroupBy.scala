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

package ai.chronon.spark.streaming

import org.slf4j.LoggerFactory
import ai.chronon
import ai.chronon.api
import ai.chronon.api.{Row => _, _}
import ai.chronon.online._
import ai.chronon.api.Extensions._
import ai.chronon.online.Extensions.ChrononStructTypeOps
import ai.chronon.spark.{EncoderUtil, GenericRowHandler}
import com.google.gson.Gson
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.Base64
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

class GroupBy(inputStream: DataFrame,
              session: SparkSession,
              groupByConf: api.GroupBy,
              onlineImpl: Api,
              debug: Boolean = false)
    extends Serializable {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  private def buildStreamingQuery(inputTable: String): String = {
    val streamingSource = groupByConf.streamingSource.get
    val query = streamingSource.query
    val selects = Option(query.selects).map(_.asScala.toMap).orNull
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val fillIfAbsent = groupByConf.dataModel match {
      case DataModel.Entities =>
        Map(Constants.TimeColumn -> timeColumn, Constants.ReversalColumn -> null, Constants.MutationTimeColumn -> null)
      case chronon.api.DataModel.Events => Map(Constants.TimeColumn -> timeColumn)
    }
    val keys = groupByConf.getKeyColumns.asScala

    val baseWheres = Option(query.wheres).map(_.asScala).getOrElse(Seq.empty[String])
    val selectMap = Option(selects).getOrElse(Map.empty[String, String])
    val keyWhereOption = keys
      .map { key =>
        s"${selectMap.getOrElse(key, key)} IS NOT NULL"
      }
      .mkString(" OR ")
    val timeWheres = groupByConf.dataModel match {
      case chronon.api.DataModel.Entities => Seq(s"${Constants.MutationTimeColumn} is NOT NULL")
      case chronon.api.DataModel.Events   => Seq(s"$timeColumn is NOT NULL")
    }
    QueryUtils.build(
      selects,
      inputTable,
      baseWheres ++ timeWheres :+ s"($keyWhereOption)",
      fillIfAbsent = if (selects == null) null else fillIfAbsent
    )
  }

  def run(local: Boolean = false): StreamingQuery = {
    buildDataStream(local).start()
  }

  // TODO: Support local by building gbServingInfo based on specified type hints when available.
  def buildDataStream(local: Boolean = false): DataStreamWriter[KVStore.PutRequest] = {
    val streamingTable = groupByConf.metaData.cleanName + "_stream"
    val fetcher = onlineImpl.buildFetcher(local)
    val groupByServingInfo = fetcher.getGroupByServingInfo(groupByConf.getMetaData.getName).get

    val streamDecoder = onlineImpl.streamDecoder(groupByServingInfo)
    assert(groupByConf.streamingSource.isDefined,
           "No streaming source defined in GroupBy. Please set a topic/mutationTopic.")
    val streamingSource = groupByConf.streamingSource.get

    val streamingQuery = buildStreamingQuery(streamingTable)

    val context = Metrics.Context(Metrics.Environment.GroupByStreaming, groupByConf)
    val ingressContext = context.withSuffix("ingress")
    import session.implicits._
    implicit val structTypeEncoder: Encoder[Mutation] = Encoders.kryo[Mutation]
    val deserialized: Dataset[Mutation] = inputStream
      .as[Array[Byte]]
      .map { arr =>
        ingressContext.increment(Metrics.Name.RowCount)
        ingressContext.count(Metrics.Name.Bytes, arr.length)
        try {
          streamDecoder.decode(arr)
        } catch {
          case ex: Throwable =>
            logger.info(
              s"Error while decoding streaming events for ${groupByConf.getMetaData.getName} with "
                + s"schema ${streamDecoder.schema.catalogString}"
                + s" \n${ex.traceString}")
            ingressContext.incrementException(ex)
            null
        }
      }
      .filter(mutation =>
        mutation != null && (!(mutation.before != null && mutation.after != null) || !(mutation.before sameElements mutation.after)))

    val streamSchema = SparkConversions.fromChrononSchema(streamDecoder.schema)
    val streamSchemaEncoder = EncoderUtil(streamSchema)
    logger.info(s"""
        | group by serving info: $groupByServingInfo
        | Streaming source: $streamingSource
        | streaming Query: $streamingQuery
        | streaming dataset: ${groupByConf.streamingDataset}
        | stream schema: $streamSchema
        |""".stripMargin)

    val des = deserialized
      .flatMap { mutation =>
        Seq(mutation.after, mutation.before)
          .filter(_ != null)
          .map(SparkConversions.toSparkRow(_, streamDecoder.schema, GenericRowHandler.func).asInstanceOf[Row])
      }(streamSchemaEncoder)
      .map { row =>
        {
          // Report flattened row count metric
          ingressContext.withSuffix("flatten").increment(Metrics.Name.RowCount)
          row
        }
      }(streamSchemaEncoder)

    des.createOrReplaceTempView(streamingTable)

    groupByConf.setups.foreach(session.sql)
    val selectedDf = session.sql(streamingQuery)
    assert(selectedDf.schema.fieldNames.contains(Constants.TimeColumn),
           s"time column ${Constants.TimeColumn} must be included in the selects")
    if (groupByConf.dataModel == api.DataModel.Entities) {
      assert(selectedDf.schema.fieldNames.contains(Constants.MutationTimeColumn), "Required Mutation ts")
    }
    val keys = groupByConf.keyColumns.asScala.toArray
    val keyIndices = keys.map(selectedDf.schema.fieldIndex)
    val (additionalColumns, eventTimeColumn) = groupByConf.dataModel match {
      case api.DataModel.Entities => Constants.MutationAvroColumns -> Constants.MutationTimeColumn
      case api.DataModel.Events   => Seq.empty[String] -> Constants.TimeColumn
    }
    val valueColumns = groupByConf.aggregationInputs ++ additionalColumns
    val valueIndices = valueColumns.map(selectedDf.schema.fieldIndex)

    val tsIndex = selectedDf.schema.fieldIndex(eventTimeColumn)
    val streamingDataset = groupByConf.streamingDataset

    val keyZSchema: api.StructType = groupByServingInfo.keyChrononSchema
    val valueZSchema: api.StructType = groupByConf.dataModel match {
      case api.DataModel.Events   => groupByServingInfo.valueChrononSchema
      case api.DataModel.Entities => groupByServingInfo.mutationValueChrononSchema
    }

    val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
    val valueToBytes = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)

    val dataWriter = new DataWriter(onlineImpl, context.withSuffix("egress"), 120, debug)
    selectedDf
      .map { row =>
        val keys = keyIndices.map(row.get)
        val values = valueIndices.map(row.get)
        val ts = row.get(tsIndex).asInstanceOf[Long]
        val keyBytes = keyToBytes(keys)
        val valueBytes = valueToBytes(values)
        if (debug) {
          val gson = new Gson()
          val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
          val pstFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("America/Los_Angeles"))
          logger.info(s"""
               |streaming dataset: $streamingDataset
               |keys: ${gson.toJson(keys)}
               |values: ${gson.toJson(values)}
               |keyBytes: ${Base64.getEncoder.encodeToString(keyBytes)}
               |valueBytes: ${Base64.getEncoder.encodeToString(valueBytes)}
               |ts: $ts  |  UTC: ${formatter.format(Instant.ofEpochMilli(ts))} | PST: ${pstFormatter.format(
            Instant.ofEpochMilli(ts))}
               |""".stripMargin)
        }
        KVStore.PutRequest(keyBytes, valueBytes, streamingDataset, Option(ts))
      }
      .writeStream
      .outputMode("append")
      .trigger(Trigger.Continuous(2.minute))
      .foreach(dataWriter)
  }
}
