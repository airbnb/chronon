package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.spark.Driver.parseConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.ScalaJavaConversions.{IterableOps, ListOps}

class Validator(tableUtils: BaseTableUtils,
               conf: Any,
               startDate: String,
               endDate: String) {

  @transient private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val range = PartitionRange(startDate, endDate)(tableUtils)

  def validateGroupBy(groupByConf: api.GroupBy): List[String] = {
    val groupBy = GroupBy.from(groupByConf, range, tableUtils, false, finalize = true)
    Validator.validatePartitionColumn(groupBy.inputDf, groupByConf.metaData.name) ++ Validator.validateTimeColumn(groupBy.inputDf, groupByConf)
  }

  def validateJoin(joinConf: api.Join)
      : List[String] = {
    val errors = joinConf.joinParts.toScala.parallel.map { part =>
      validateGroupBy(part.groupBy)
    }.reduce(_ ++ _)
    val leftDf = JoinUtils.leftDf(joinConf, range, tableUtils, allowEmpty = true).get
    errors ++ Validator.validatePartitionColumn(leftDf, "leftDf")
  }

  def run(): Unit = {
    val errors: List[String] = conf match {
      case confPath: String =>
        if (confPath.contains("joins")) {
          val joinConf = parseConf[api.Join](confPath)
          validateJoin(joinConf)
        } else if (confPath.contains("group_bys")) {
          val groupByConf = parseConf[api.GroupBy](confPath)
          validateGroupBy(groupByConf)
        } else {
          throw new Exception(s"confPath $confPath should contain joins or group_bys")
        }
      case groupByConf: api.GroupBy => validateGroupBy(groupByConf)
      case joinConf: api.Join       => validateJoin(joinConf)
    }
    if (errors.nonEmpty) {
      logger.error("The following errors were found:\n" + errors.mkString("\n"))
    }
  }
}

object Validator {

  @transient private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)
  def validatePartitionColumn(df: DataFrame, dfName: String): List[String] = {
    val schema = df.schema
    if (!schema.names.contains(Constants.PartitionColumn)) {
      // A datasource can be unpartitioned and not contain the partition column.
      logger.info(s"df for ${dfName} does not have a PartitionColumn ${Constants.PartitionColumn} so will be treated as unpartitioned")
      return List.empty
    }
    if (schema(Constants.PartitionColumn).dataType != StringType) {
      return List(s"df for ${dfName} has wrong type for PartitionColumn ${Constants.PartitionColumn}, should be StringType")
    }
    val dayColumnSample = df.select(col(Constants.PartitionColumn)).take(100).map(_.getString(0))
    val formatter = DateTimeFormatter.ofPattern(Constants.Partition.format)
    dayColumnSample.map { partition =>
      try {
        LocalDate.parse(partition, formatter)
      } catch {
        case _: Throwable =>
          return List(s"df for ${dfName} has wrong format for PartitionColumn ${Constants.PartitionColumn}, should be ${Constants.Partition.format}")
      }
    }
    List.empty
  }

  def validateTimeColumn(df: DataFrame, groupByConf: api.GroupBy): List[String] = {
    if (!GroupBy.needsTime(groupByConf)) {
      return List.empty
    }
    val schema = df.schema
    if (!schema.names.contains(Constants.TimeColumn)) {
      return List(s"df for groupBy ${groupByConf.metaData.name} does not contain TimeColumn " + Constants.TimeColumn)
    }
    if (schema(Constants.TimeColumn).dataType != LongType) {
      return List(s"df for groupBy ${groupByConf.metaData.name} has wrong type for TimeColumn ${Constants.TimeColumn}, should be LongType")
    }
    val timeColumnSample = df.select(col(Constants.TimeColumn)).where(col(Constants.TimeColumn).isNotNull).take(100).map(_.getLong(0))
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    // Assume timestamps are in some bounded range. We may need to tweak this logic if there are use cases for running
    // on data before year 2000 (there shouldn't be for Stripe). This is mainly checking for timestamps that are
    // in the wrong unit (ex. seconds instead of milliseconds since Unix epoch) so we only need to check a small
    // number of timestamps.
    val lowerBound = LocalDate.parse("20001010", formatter).atStartOfDay(ZoneId.of("UTC")).toInstant.toEpochMilli
    val upperBound = LocalDate.parse("25000101", formatter).atStartOfDay(ZoneId.of("UTC")).toInstant.toEpochMilli
    timeColumnSample.map { ts =>
      if (ts <= lowerBound || ts >= upperBound) {
        return List(s"df for groupBy ${groupByConf.metaData.name} should have TimeColumn ${Constants.TimeColumn} that is milliseconds since Unix epoch. Example invalid ts: $ts")
      }
    }
    List.empty
  }
}
