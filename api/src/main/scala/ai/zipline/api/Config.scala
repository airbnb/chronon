//package ai.zipline.api
//
//import java.io.{File, PrintWriter}
//
//import com.sksamuel.avro4s.{AvroInputStream, AvroSchema, Decoder, SchemaFor}
//
//import scala.io.{Source => FileSource}
//import scala.reflect.runtime.universe.{TypeTag, typeOf}
//
//object Config {
//  case class TimeUnit(millis: Long, str: String)
//
//  object TimeUnit {
//    val Hours: TimeUnit = TimeUnit(3600 * 1000, "h")
//    val Days: TimeUnit = TimeUnit(24 * 3600 * 1000, "d")
//  }
//
//  case class Window(length: Int, unit: TimeUnit) extends Serializable {
//    val str: String =
//      if (length == Int.MaxValue) "unbounded" else s"$length${unit.str}"
//
//    val suffix: String =
//      if (length == Int.MaxValue) "" else s"_$length${unit.str}"
//
//    val millis: Long = length.toLong * unit.millis
//  }
//
//  object Window {
//    val Unbounded: Window = Window(Int.MaxValue, TimeUnit.Days)
//    val Hour: Window = Window(1, TimeUnit.Hours)
//    val Day: Window = Window(1, TimeUnit.Days)
//    private val SecondMillis: Long = 1000
//    private val Minute: Long = 60 * SecondMillis
//    val FiveMinutes: Long = 5 * Minute
//
//    def millisToString(millis: Long): String = {
//      if (millis % Day.millis == 0) {
//        Window((millis / Day.millis).toInt, TimeUnit.Days).str
//      } else if (millis % Hour.millis == 0) {
//        Window((millis / Hour.millis).toInt, TimeUnit.Hours).str
//      } else if (millis % Minute == 0) {
//        s"${millis / Minute}mins"
//      } else if (millis % SecondMillis == 0) {
//        s"${millis / SecondMillis}secs"
//      } else {
//        s"${millis}ms"
//      }
//    }
//  }
//
//  object DataModel extends Enumeration {
//    type DataModel = Value
//    val Entities, Events = Value
//  }
//  import DataModel._
//
//  object Accuracy extends Enumeration {
//    type Accuracy = Value
//    val Snapshot, Temporal = Value
//  }
//
//  import Accuracy._
//  // stuff that is common to join, groupBy and staging query
//  case class MetaData(name: String,
//                      team: String,
//                      dependencies: Seq[String] = Seq.empty[String],
//                      // conf can't change once marked online
//                      // we will fail re-generation of the conf at the site of creation
//                      online: Boolean = false,
//                      // triggers alerts when SLAs are breached
//                      // library changes are released to staging copies when possible
//                      production: Boolean = false) {
//    val cleanName: String = name.replaceAll("[^a-zA-Z0-9_]", "_")
//  }
//
//  object Constants {
//    val TimeColumn: String = "ts"
//    val PartitionColumn: String = "ds"
//    val TimePartitionColumn: String = "ts_ds"
//    val ReversalColumn: String = "is_before"
//    val MutationTimeColumn: String = "mutation_ts"
//    val Partition: PartitionSpec =
//      PartitionSpec(format = "yyyy-MM-dd", spanMillis = Window.Day.millis)
//    val StartPartitionMacro = "[START_PARTITION]"
//    val EndPartitionMacro = "[END_PARTITION]"
//  }
//
//  case class PartitionSpec(format: String, spanMillis: Long)
//
//  object Operation extends Enumeration {
//    type Operation = Value
//    val Sum, Average, Count, ApproxDistinctCount, First, Last, Min, Max, FirstK, LastK, TopK, BottomK = Value
//  }
//
//  import ai.zipline.api.Config.Operation._
//
//  // one per output column - so single window
//  // not exposed to users
//  case class AggregationPart(operation: Operation,
//                             inputColumn: String,
//                             window: Window = Window.Unbounded,
//                             private val argMap: Map[String, String] = Map.empty[String, String])
//      extends Serializable {
//
//    def getInt(arg: String): Int = {
//      val argOpt = argMap.get(arg)
//      require(
//        argOpt.isDefined,
//        s"$arg needs to be specified in the constructor json for ${operation} type"
//      )
//      argOpt.get.toInt
//    }
//
//    private def opSuffix =
//      operation match {
//        case LastK   => s"last${getInt("k")}"
//        case FirstK  => s"first${getInt("k")}"
//        case TopK    => s"top${getInt("k")}"
//        case BottomK => s"bottom${getInt("k")}"
//        case other   => other.toString.toLowerCase
//      }
//
//    def outputColumnName = s"${inputColumn}_$opSuffix${window.suffix}"
//  }
//  case class Aggregation(operation: Operation,
//                         inputColumn: String,
//                         windows: Seq[Window] = null,
//                         private val argMap: Map[String, String] = Map.empty[String, String]) {
//
//    def unpack: Seq[AggregationPart] =
//      Option(windows)
//        .getOrElse(Seq(Window.Unbounded))
//        .map { window =>
//          Option(window) match {
//            case Some(window) => AggregationPart(operation, inputColumn, window, argMap)
//            case None         => AggregationPart(operation, inputColumn, Window.Unbounded, argMap)
//          }
//        }
//
//    def unWindowed: AggregationPart = AggregationPart(operation, inputColumn, Window.Unbounded, argMap)
//
//  }
//
//  case class Query(table: String,
//                   selects: Seq[String] = null,
//                   wheres: Seq[String] = null, // joined by an 'AND' qualifier
//                   startPartition: String = null,
//                   endPartition: String = null,
//                   timeExpression: String = null)
//
//  case class Source(query: Query,
//                    dataModel: DataModel,
//                    topic: String = null,
//                    mutationTable: String = null,
//                    mutationTimeExpression: String = Constants.MutationTimeColumn,
//                    reversalExpression: String = Constants.ReversalColumn)
//
//  case class GroupBy(sources: Seq[Source],
//                     keys: Seq[String],
//                     // null means already aggregated - so we simply join
//                     aggregations: Seq[Aggregation],
//                     metadata: MetaData)
//
//  // when the datasource has topic and left side is events, the join will be temporally accurate
//  // otherwise the join will always be snapshot accurate
//  case class JoinPart(groupBy: GroupBy,
//                      // what columns in the left side should map to what columns on the right side
//                      keyRenaming: Map[String, String] = null,
//                      // accuracy defaulted based on context
//                      // left entities => accuracy has to be "snapshot" or null
//                      // left events, right entities => default/null accuracy is inferred as "snapshot"
//                      // left events, right events => default/null accuracy is inferred as "temporal"
//                      accuracy: Accuracy = null,
//                      // Sometimes you want to join to the same feature_set multiple times
//                      // In those cases you can use the prefix to distinguish
//                      prefix: String = null)
//
//  case class Join(scanQuery: Query,
//                  dataModel: DataModel,
//                  joinParts: Seq[JoinPart],
//                  metadata: MetaData,
//                  timeExpression: String = Constants.TimeColumn) {}
//
//  // TODO: Add support
//  case class Staging(query: String, startPartition: String, metadata: MetaData, partitionSpec: PartitionSpec)
//
//  // TODO: Convert avsc files to python files using https://github.com/srserves85/avro-to-python
//  def writeSchema[T: SchemaFor: TypeTag](folderPath: String): Unit = {
//    val typeName = typeOf[T].typeSymbol.name.toString
//    val outputPath = s"$folderPath/$typeName.avsc"
//    val contents = AvroSchema[T].toString(true)
//    println(contents)
//    val writer = new PrintWriter(new File(outputPath))
//    writer.write(contents)
//    writer.close()
//    println(s"Wrote Schema for $typeName to $outputPath with content:")
//    FileSource.fromFile(outputPath).foreach { x => print(x) }
//    println()
//  }
//
//  def parseFile[T: Decoder: SchemaFor: TypeTag](filePath: String): T = {
//    val typeName = typeOf[T].typeSymbol.name.toString
//    println(s"Parsing $typeName from file $filePath - expecting avro json serialization")
//    val schema = AvroSchema[T]
//    val inputStream = AvroInputStream.json[T].from(filePath).build(schema)
//    val objs = inputStream.iterator.toList
//    inputStream.close()
//    objs.head
//  }
//
//  def main(args: Array[String]): Unit = {
//    println(args.mkString(","))
//    val outputFolder = args(0)
//    writeSchema[GroupBy](outputFolder)
//    writeSchema[Join](outputFolder)
//  }
//}
