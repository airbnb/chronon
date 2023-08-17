package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import ai.chronon.api.{Constants, GroupByServingInfo, StructType}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.SparkConversions.toChrononType
import ai.chronon.online.{AvroConversions, GroupByServingInfoParsed, KVStore, SparkConversions}
import ai.chronon.spark.{GenericRowHandler, GroupBy, PartitionRange, TableUtils}
import com.google.gson.Gson
import org.apache.spark.sql.types.{StructType => SparkStruct}
import org.apache.spark.sql.{Row, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.Base64
import scala.util.ScalaJavaConversions.ListOps

case class PutRequestBuilder(keyIndices: Array[Int],
                             valueIndices: Array[Int],
                             tsIndex: Int,
                             keySchema: api.StructType,
                             valueSchema: api.StructType,
                             dataset: String)
    extends Serializable {
  @transient lazy val keyToBytes: Any => Array[Byte] = AvroConversions.encodeBytes(keySchema, GenericRowHandler.func)
  @transient lazy val valueToBytes: Any => Array[Byte] =
    AvroConversions.encodeBytes(valueSchema, GenericRowHandler.func)
  def from(row: Row, debug: Boolean): PutRequest = {
    val keys = keyIndices.map(row.get)
    val values = valueIndices.map(row.get)
    val ts = row.get(tsIndex).asInstanceOf[Long]
    val keyBytes = keyToBytes(keys)
    val valueBytes = valueToBytes(values)
    if (debug) {
      val gson = new Gson()
      val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
      val pstFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("America/Los_Angeles"))
      println(s"""
           |keys: ${gson.toJson(keys)}
           |values: ${gson.toJson(values)}
           |keyBytes: ${Base64.getEncoder.encodeToString(keyBytes)}
           |valueBytes: ${Base64.getEncoder.encodeToString(valueBytes)}
           |ts: $ts|  UTC: ${formatter.format(Instant.ofEpochMilli(ts))}| PST: ${pstFormatter.format(
        Instant.ofEpochMilli(ts))}
           |""".stripMargin)
    }
    KVStore.PutRequest(keyBytes, valueBytes, dataset, Option(ts))
  }
}

object PutRequestBuilder {

  def from(groupByConf: api.GroupBy, session: SparkSession): PutRequestBuilder = {
    val keys: Array[String] = groupByConf.keyColumns.toScala.toArray
    implicit val tableUtils: TableUtils = TableUtils(session)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

    // TODO: Fix mutation case here
    val groupBy = ai.chronon.spark.GroupBy.from(groupByConf,
                                                PartitionRange(today, today),
                                                TableUtils(session),
                                                computeDependency = false,
                                                mutationScan = false)

    val inputSchema: SparkStruct = groupBy.inputDf.schema
    def inputFieldIndex(s: String): Int = inputSchema.fieldIndex(s)

    val keyIndices: Array[Int] = keys.map(inputFieldIndex)
    val (additionalColumns, eventTimeColumn) = groupByConf.dataModel match {
      case api.DataModel.Entities => Constants.MutationAvroColumns -> Constants.MutationTimeColumn
      case api.DataModel.Events   => Seq.empty[String] -> Constants.TimeColumn
    }
    val valueColumns: Array[String] = groupByConf.aggregationInputs ++ additionalColumns
    val valueIndices: Array[Int] = valueColumns.map(inputFieldIndex)

    val tsIndex: Int = inputFieldIndex(eventTimeColumn)
    val streamingDataset: String = groupByConf.streamingDataset

    def toChrononSchema(name: String, schema: SparkStruct): api.StructType =
      api.StructType.from(name, SparkConversions.toChrononSchema(schema))

    val keyZSchema: api.StructType = toChrononSchema("key", groupBy.keySchema)
    lazy val valueChrononSchema: api.StructType = {
      val valueFields = groupByConf.aggregationInputs
        .flatMap(inp => inputSchema.fields.find(_.name == inp))
        .map(field => api.StructField(field.name, toChrononType(field.name, field.dataType)))
      api.StructType(s"${groupByConf.metaData.cleanName}_INPUT_COLS", valueFields)
    }
    lazy val mutationValueChrononSchema: api.StructType = api.StructType(
      s"${groupByConf.metaData.cleanName}_MUTATION_COLS",
      (valueChrononSchema ++ Constants.MutationAvroFields).toArray)

    val valueZSchema: api.StructType = groupByConf.dataModel match {
      case api.DataModel.Events   => valueChrononSchema
      case api.DataModel.Entities => mutationValueChrononSchema
    }
    PutRequestBuilder(keyIndices, valueIndices, tsIndex, keyZSchema, valueZSchema, streamingDataset)
  }

  def buildProxyServingInfo(groupByConf: api.GroupBy, session: SparkSession): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    implicit val tableUtils: TableUtils = TableUtils(session)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val groupBy = ai.chronon.spark.GroupBy
      .from(groupByConf, PartitionRange(today, today), TableUtils(session), computeDependency = false)
    groupByServingInfo.setBatchEndDate(today)
    groupByServingInfo.setGroupBy(groupByConf)
    groupByServingInfo.setKeyAvroSchema(groupBy.keySchema.toAvroSchema("Key").toString(true))
    groupByServingInfo.setSelectedAvroSchema(groupBy.preAggSchema.toAvroSchema("Value").toString(true))
    if (groupByConf.streamingSource.isDefined) {
      val streamingSource = groupByConf.streamingSource.get
      val fullInputSchema = tableUtils.getSchemaFromTable(streamingSource.table)
      val streamingQuery = groupByConf.buildStreamingQuery
      val inputSchema: SparkStruct =
        if (Option(streamingSource.query.selects).isEmpty) fullInputSchema
        else {
          val reqColumns = tableUtils.getColumnsFromQuery(streamingQuery)
          SparkStruct(fullInputSchema.filter(col => reqColumns.contains(col.name)))
        }
      groupByServingInfo.setInputAvroSchema(inputSchema.toAvroSchema(name = "Input").toString(true))
    } else {
      println("Not setting InputAvroSchema to GroupByServingInfo as there is no streaming source defined.")
    }
    new GroupByServingInfoParsed(groupByServingInfo, tableUtils.partitionSpec)
  }
}
