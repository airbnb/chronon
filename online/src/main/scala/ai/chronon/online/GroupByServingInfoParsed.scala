package ai.chronon.online

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing.SawtoothOnlineAggregator
import ai.chronon.api.Constants.{ReversalField, TimeField}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.api._
import org.apache.avro.Schema

import scala.collection.JavaConverters.asScalaBufferConverter

// mixin class - with schema
class GroupByServingInfoParsed(groupByServingInfo: GroupByServingInfo, partitionSpec: PartitionSpec)
    extends GroupByServingInfo(groupByServingInfo)
    with Serializable {

  // streaming starts scanning after batchEnd
  lazy val batchEndTsMillis: Long = partitionSpec.epochMillis(batchEndDate)
  private def parser = new Schema.Parser()

  val MutationAvroFields: Seq[StructField] = Seq(TimeField, ReversalField)
  val MutationAvroColumns: Seq[String] = MutationAvroFields.map(_.name)

  lazy val aggregator: SawtoothOnlineAggregator = {
    new SawtoothOnlineAggregator(batchEndTsMillis,
                                 groupByServingInfo.groupBy.aggregations.asScala.toSeq,
                                 valueChrononSchema.fields.map(sf => (sf.name, sf.fieldType)))
  }

  // caching groupBy helper to avoid re-computing batchDataSet,streamingDataset & inferred accuracy
  lazy val groupByOps = new GroupByOps(groupByServingInfo.groupBy)

  lazy val irChrononSchema: StructType =
    StructType.from(s"${groupBy.metaData.cleanName}_IR", aggregator.batchIrSchema)

  def keyCodec: AvroCodec = AvroCodec.of(keyAvroSchema)
  @transient lazy val keyChrononSchema: StructType =
    AvroConversions.toChrononSchema(keyCodec.schema).asInstanceOf[StructType]

  lazy val valueChrononSchema: StructType = {
    val valueFields = groupBy.aggregationInputs
      .flatMap(inp => selectedChrononSchema.fields.find(_.name == inp))
    StructType(s"${groupBy.metaData.cleanName}_INPUT_COLS", valueFields)
  }

  lazy val valueAvroSchema: String = {
    AvroConversions.fromChrononSchema(valueChrononSchema).toString()
  }

  def valueAvroCodec: AvroCodec = AvroCodec.of(valueAvroSchema)
  def selectedCodec: AvroCodec = AvroCodec.of(selectedAvroSchema)
  lazy val irAvroSchema: String = AvroConversions.fromChrononSchema(irChrononSchema).toString()
  def irCodec: AvroCodec = AvroCodec.of(irAvroSchema)
  def outputCodec: AvroCodec = AvroCodec.of(outputAvroSchema)


  // Start tiling specific variables

  lazy val tiledCodec: TileCodec = new TileCodec(groupBy, valueChrononSchema.fields.map(sf => (sf.name, sf.fieldType)))
  lazy val isTilingEnabled: Boolean = TileCodec.isTilingEnabled(groupBy)

  // End tiling specific variables

  def outputChrononSchema: StructType = {
    StructType.from(s"${groupBy.metaData.cleanName}_OUTPUT", aggregator.windowedAggregator.outputSchema)
  }

  lazy val outputAvroSchema: String = { AvroConversions.fromChrononSchema(outputChrononSchema).toString() }

  def inputChrononSchema: StructType = {
    AvroConversions.toChrononSchema(parser.parse(inputAvroSchema)).asInstanceOf[StructType]
  }

  def selectedChrononSchema: StructType = {
    AvroConversions.toChrononSchema(parser.parse(selectedAvroSchema)).asInstanceOf[StructType]
  }

  // Schema associated to the stored KV value for streaming data.
  // Mutations require reversal column and timestamp for proper computation and the effective timestamp of mutations is
  // the MutationTime. Therefore, the schema in the value now includes timestamp and reversal column.}
  lazy val mutationValueAvroSchema: String = {
    AvroConversions
      .fromChrononSchema(
        StructType(s"${groupBy.metaData.cleanName}_MUTATION_COLS", (valueChrononSchema ++ MutationAvroFields).toArray))
      .toString
  }

  def mutationValueChrononSchema: StructType = {
    AvroConversions.toChrononSchema(parser.parse(mutationValueAvroSchema)).asInstanceOf[StructType]
  }

  def mutationValueAvroCodec: AvroCodec = AvroCodec.of(mutationValueAvroSchema)

  // Schema for data consumed by the streaming job.
  // Needs consistency with mutationDf Schema for backfill group by. (Shared queries)
  // Additional columns used for mutations are stored
  def mutationChrononSchema: StructType = {
    val fields: scala.collection.Seq[StructField] = inputChrononSchema ++ Constants.MutationFields
    StructType("MUTATION_SCHEMA", fields.toArray)
  }

  def streamChrononSchema: StructType = {
    groupByOps.dataModel match {
      case DataModel.Events   => inputChrononSchema
      case DataModel.Entities => mutationChrononSchema
    }
  }
}
