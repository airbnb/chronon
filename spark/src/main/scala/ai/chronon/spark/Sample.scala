package ai.chronon.spark

import scala.jdk.CollectionConverters.asScalaBufferConverter

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.spark.Driver.parseConf

class Sample(conf: Any,
             tableUtils: TableUtils,
             numRows: Option[Int] = None,
             startDate: String,
             endDate: String) {


  def sampleSource(table: String, dateRange: PartitionRange, keySetOpt: Option[Map[String, Set[String]]] = None) = {

    val outputDirectory = s"./$table"

    val additionalFilterString = keySetOpt.map{ keySet =>
      val filterList = keySet.map{ case(keyName: String, valueSet: Set[String]) =>
        val wrappedValueSet = valueSet.map(value => s"'$value'")
        s"$keyName in (${wrappedValueSet.mkString(sep=",")})"
      }.mkString(sep=" AND \n")
      s" AND ${filterList.mkString(sep = " AND \n")}"
    }.getOrElse(numRows.map( num => s" LIMIT $num").getOrElse(""))

    val sql =
      s"""
        |SELECT * FROM $table
        |WHERE ds >= ${dateRange.start} AND
        |ds <= ${dateRange.end}
        |$additionalFilterString
        |""".stripMargin

    print(s"Gathering Data with Query: $sql \n\n Writing to: $outputDirectory")

    tableUtils.sparkSession.sql(sql).write.mode("overwrite").parquet(outputDirectory)
  }

  /*
  For a given GroupBy, sample all the sources. Can optionally take a set of keys to use for sampling
  (used by the join sample path).
   */
  def sampleGroupBy(groupBy: api.GroupBy, keySetOpt: Option[Map[String, Set[String]]] = None) = {
    // Get a list of source table and relevant partition range for the ds being run
    groupBy.sources.asScala.foreach { source =>
      val range: PartitionRange = GroupBy.getIntersectedRange(
        source,
        PartitionRange(startDate, endDate)(tableUtils),
        tableUtils,
        groupBy.maxWindow)
      sampleSource(source.rootTable, range, keySetOpt)
    }
  }


  def sampleJoin(join: api.Join) = {
    // First sample the left side
    sampleSource(join.getLeft.rootTable, PartitionRange(startDate, endDate)(tableUtils))

    

  }

  def run(): Unit =
    conf match {
      case confPath: String =>
        if (confPath.contains("/joins/")) {
          val joinConf = parseConf[api.Join](confPath)
          sampleJoin(joinConf)
        } else if (confPath.contains("/group_bys/")) {
          val groupByConf = parseConf[api.GroupBy](confPath)
          sampleGroupBy(groupByConf)
        }
      case groupByConf: api.GroupBy => sampleGroupBy(groupByConf)
      case joinConf: api.Join       => sampleJoin(joinConf)
    }
}
