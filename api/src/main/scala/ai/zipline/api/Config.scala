package ai.zipline.api

import java.io.{File, PrintWriter}

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}

import scala.io.Source
import scala.reflect.runtime.universe.{TypeTag, typeOf}

object Config {
  case class TimeUnit(millis: Long, str: String)

  object TimeUnit {
    val Hours: TimeUnit = TimeUnit(3600 * 1000, "h")
    val Days: TimeUnit = TimeUnit(24 * 3600 * 1000, "d")
  }

  case class Window(length: Int, unit: TimeUnit) extends Serializable {
    val toSuffix: String =
      if (length == Int.MaxValue) "" else s"_$length${unit.str}"

    val millis: Long = length.toLong * unit.millis
  }

  object Window {
    val Infinity: Window = Window(Int.MaxValue, TimeUnit.Days)
    val Hour: Window = Window(1, TimeUnit.Hours)
    val Day: Window = Window(1, TimeUnit.Days)
    val FiveMinutes: Long = 5 * 60 * 1000L
  }

  object DataModel extends Enumeration {
    type DataModel = Value
    val Entities, Events = Value
  }
  import DataModel._

  object Accuracy extends Enumeration {
    type Accuracy = Value
    val Snapshot, Temporal = Value
  }

  import Accuracy._
  // stuff that is common to join, groupBy and staging query
  case class MetaData(name: String,
                      team: String,
                      dependencies: Seq[String],
                      // conf can't change once marked online
                      // we will fail re-generation of the conf at the site of creation
                      online: Boolean = false,
                      // triggers alerts when SLAs are breached
                      // library changes are released to staging copies when possible
                      production: Boolean = false)

  object Constants {
    val TimeColumn: String = "ts"
    val PartitionColumn: String = "ds"
    val ReversalColumn: String = "is_before"
    val MutationTimeColumn: String = "mutation_ts"
    val Partition: PartitionSpec =
      PartitionSpec(format = "yyyy-MM-dd", spanMillis = Window.Day.millis)
    val StartPartitionMacro = "[START_PARTITION]"
    val EndPartitionMacro = "[END_PARTITION]"
  }

  case class PartitionSpec(format: String, spanMillis: Long)

  object AggregationType extends Enumeration {
    type AggregationType = Value
    val Sum, Average, Count, ApproxDistinctCount, First, Last, Min, Max, FirstK, LastK, TopK, BottomK = Value
  }

  import ai.zipline.api.Config.AggregationType._

  // one per output column - so single window
  case class AggregationPart(`type`: AggregationType,
                             inputColumn: String,
                             window: Window = Window.Infinity,
                             private val args: Map[String, String] = Map.empty[String, String])
      extends Serializable {

    def getInt(arg: String): Int = {
      val argOpt = args.get(arg)
      require(
        argOpt.isDefined,
        s"$arg needs to be specified in the constructor json for ${`type`} type"
      )
      argOpt.get.toInt
    }

    private def opSuffix =
      `type` match {
        case LastK   => s"last_${getInt("k")}"
        case FirstK  => s"first_${getInt("k")}"
        case TopK    => s"top_${getInt("k")}"
        case BottomK => s"bottom_${getInt("k")}"
        case other   => other.toString.toLowerCase
      }

    def outputColumnName = s"${inputColumn}_$opSuffix${window.toSuffix}"
  }
  case class Aggregation(`type`: AggregationType,
                         inputColumn: String,
                         windows: Seq[Window],
                         private val args: Map[String, String] = Map.empty[String, String]) {

    def unpack: Seq[AggregationPart] =
      Option(windows)
        .getOrElse(Seq(Window.Infinity))
        .map { window =>
          Option(window) match {
            case Some(window) => AggregationPart(`type`, inputColumn, window, args)
            case None         => AggregationPart(`type`, inputColumn, Window.Infinity, args)
          }
        }

    def unWindowed: AggregationPart = AggregationPart(`type`, inputColumn, Window.Infinity, args)

  }

  case class DataSource(selects: Seq[String],
                        wheres: Seq[String],
                        table: String,
                        dataModel: DataModel,
                        topic: String,
                        mutationTable: String,
                        startPartition: String,
                        endPartition: String = null,
                        timeExpression: String = Constants.TimeColumn,
                        mutationTimeExpression: String = Constants.MutationTimeColumn,
                        reversalExpression: String = Constants.ReversalColumn)

  case class GroupBy(sources: Seq[DataSource],
                     keys: Seq[String],
                     // null means already aggregated - so we simply join
                     aggregations: Seq[Aggregation],
                     metadata: MetaData)

  case class JoinPart(groupBy: GroupBy,
                      accuracy: Accuracy.Accuracy,
                      // what columns in the should map to what columns in the
                      keyRenaming: Map[String, String],
                      // GroupBy names can get large - use this if you want to replace those
                      // Sometimes you want to join to the same feature_set multiple times
                      // In those cases you can use the prefix to distinguish
                      prefix: String)

  case class Join(query: String,
                  table: String,
                  dataModel: DataModel,
                  startPartition: String,
                  joinParts: Seq[JoinPart],
                  metadata: MetaData,
                  timeExpression: String = Constants.TimeColumn)

  // TODO: Add support
  case class Staging(query: String, startPartition: String, metadata: MetaData, partitionSpec: PartitionSpec)

  def writeSchema[T: SchemaFor: TypeTag](folderPath: String): Unit = {
    val typeName = typeOf[T].typeSymbol.name.toString
    val outputPath = s"$folderPath/$typeName.avsc"
    val contents = AvroSchema[T].toString(true)
    println(contents)
    val writer = new PrintWriter(new File(outputPath))
    writer.write(contents)
    writer.close()
    println(s"Wrote Schema for $typeName to $outputPath with content:")
    Source.fromFile(outputPath).foreach { x => print(x) }
    println()

  }

  def main(args: Array[String]): Unit = {
    println(args.mkString(","))
    val outputFolder = args(0)
    writeSchema[GroupBy](outputFolder)
    writeSchema[Join](outputFolder)
  }
}
