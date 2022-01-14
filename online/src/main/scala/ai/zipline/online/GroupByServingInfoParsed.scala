package ai.zipline.online

import ai.zipline.aggregator.windowing.SawtoothOnlineAggregator
import ai.zipline.api.Extensions.{GroupByOps, MetadataOps}
import ai.zipline.api.{Constants, GroupByServingInfo, StructType}
import org.apache.avro.Schema

import scala.collection.JavaConverters.asScalaBufferConverter

// mixin class - with schema
class GroupByServingInfoParsed(groupByServingInfo: GroupByServingInfo)
    extends GroupByServingInfo(groupByServingInfo)
    with Serializable {

  // streaming starts scanning after batchEnd
  lazy val batchEndTsMillis: Long = Constants.Partition.epochMillis(batchEndDate)
  private def parser = new Schema.Parser()

  lazy val aggregator: SawtoothOnlineAggregator = {
    new SawtoothOnlineAggregator(batchEndTsMillis,
                                 groupByServingInfo.groupBy.aggregations.asScala,
                                 valueZiplineSchema.fields.map(sf => (sf.name, sf.fieldType)))
  }

  // caching groupBy helper to avoid re-computing batchDataSet,streamingDataset & inferred accuracy
  lazy val groupByOps = new GroupByOps(groupByServingInfo.groupBy)

  lazy val irZiplineSchema: StructType =
    StructType.from(s"${groupBy.metaData.cleanName}_IR", aggregator.batchIrSchema)

  def keyCodec: AvroCodec = AvroCodec.of(keyAvroSchema)

  lazy val valueZiplineSchema: StructType = {
    val valueFields = groupBy.aggregationInputs
      .flatMap(inp => selectedZiplineSchema.fields.find(_.name == inp))
    StructType(s"${groupBy.metaData.cleanName}_INPUT_COLS", valueFields)
  }

  lazy val valueAvroSchema: String = {
    AvroUtils.fromZiplineSchema(valueZiplineSchema).toString()
  }

  def valueAvroCodec: AvroCodec = AvroCodec.of(valueAvroSchema)
  def selectedCodec: AvroCodec = AvroCodec.of(selectedAvroSchema)
  lazy val irAvroSchema: String = AvroUtils.fromZiplineSchema(irZiplineSchema).toString()
  def irCodec: AvroCodec = AvroCodec.of(irAvroSchema)
  def outputCodec: AvroCodec = AvroCodec.of(outputAvroSchema)

  lazy val outputAvroSchema: String = {
    val outputZiplineSchema =
      StructType.from(s"${groupBy.metaData.cleanName}_OUTPUT", aggregator.windowedAggregator.outputSchema)
    AvroUtils.fromZiplineSchema(outputZiplineSchema).toString()
  }

  def inputZiplineSchema: StructType = {
    AvroUtils.toZiplineSchema(parser.parse(inputSparkSchema)).asInstanceOf[StructType]
  }

  def selectedZiplineSchema: StructType = {
    AvroUtils.toZiplineSchema(parser.parse(selectedAvroSchema)).asInstanceOf[StructType]
  }
}
