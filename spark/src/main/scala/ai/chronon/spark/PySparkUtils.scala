package ai.chronon.spark

import ai.chronon.aggregator.windowing.{FiveMinuteResolution, Resolution}
import ai.chronon.api
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{ConstantNameProvider, Constants, ThriftJsonCodec}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object PySparkUtils {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Pyspark has a tough time creating a FiveMinuteResolution via jvm.ai.chronon.aggregator.windowing.FiveMinuteResolution so we provide this helper method
   *
   * @return FiveMinuteResolution
   */

  def getFiveMinuteResolution: Resolution = FiveMinuteResolution

  /**
   * Creating optionals is difficult to support in Pyspark, so we provide this method as a work around
   *
   * @param timeRange a time range
   * @return Empty time range optional
   */
  def getTimeRangeOptional(timeRange: TimeRange): Option[TimeRange] = if (timeRange == null) Option.empty[TimeRange] else Option(timeRange)

  /**
   * Creating optionals is difficult to support in Pyspark, so we provide this method as a work around
   *
   * @param str a string
   * @return String optional
   */
  def getStringOptional(str: String): Option[String] = if (str == null) Option.empty[String] else Option(str)

  /**
   * Creating optionals is difficult to support in Pyspark, so we provide this method as a work around
   * Furthermore, ints can't be null in Scala so we need to pass the value in as a str
   *
   * @param strInt a string
   * @return Int optional
   */
  def getIntOptional(strInt: String): Option[Int] = if (strInt == null) Option.empty[Int] else Option(strInt.toInt)

  /**
   * Type parameters are difficult to support in Pyspark, so we provide these helper methods for ThriftJsonCodec.fromJsonStr
   *
   * @param groupByJson a JSON string representing a group by
   * @return Chronon Scala API GroupBy object
   */
  def parseGroupBy(groupByJson: String): api.GroupBy = {
    ThriftJsonCodec.fromJsonStr[api.GroupBy](groupByJson, check = true, classOf[api.GroupBy])
  }

  /**
   * Type parameters are difficult to support in Pyspark, so we provide these helper methods for ThriftJsonCodec.fromJsonStr
   *
   * @param joinJson a JSON string representing a join
   * @return Chronon Scala API Join object
   */
  def parseJoin(joinJson: String): api.Join = {
    ThriftJsonCodec.fromJsonStr[api.Join](joinJson, check = true, classOf[api.Join])
  }

  /**
   * Type parameters are difficult to support in Pyspark, so we provide these helper methods for ThriftJsonCodec.fromJsonStr
   *
   * @param sourceJson a JSON string representing a source.
   * @return Chronon Scala API Source object
   */
  def parseSource(sourceJson: String): api.Source = {
    ThriftJsonCodec.fromJsonStr[api.Source](sourceJson, check = true, classOf[api.Source])
  }

  /**
   * Helper function to get Temporal or Snapshot Accuracy
   *
   * @param getTemporal boolean value that will decide if we return temporal or snapshot accuracy .
   * @return api.Accuracy
   */
  def getAccuracy(getTemporal: Boolean): api.Accuracy = {
    if (getTemporal) api.Accuracy.TEMPORAL else api.Accuracy.SNAPSHOT
  }

  /**
   * Helper function to allow a user to execute a Group By.
   *
   * @param groupByConf       api.GroupBy Chronon scala GroupBy API object
   * @param endDate           str this represents the last date we will perform the aggregation for
   * @param stepDays          int this will determine how we chunk filling the missing partitions
   * @param tableUtils        TableUtils this will be used to perform ops against our data sources
   * @param constantsProvider ConstantsProvider must be set from the Scala side. Doing so from PySpark will not properly set it.
   * @return DataFrame
   */
  def runGroupBy(groupByConf: api.GroupBy, endDate: String, stepDays: Option[Int], tableUtils: BaseTableUtils, constantsProvider: ConstantNameProvider): DataFrame = {
    logger.info(s"Executing GroupBy: ${groupByConf.metaData.name}")
    Constants.initConstantNameProvider(constantsProvider)
    GroupBy.computeBackfill(
      groupByConf,
      endDate,
      tableUtils,
      stepDays
    )
    logger.info(s"Finished executing GroupBy: ${groupByConf.metaData.name}")
    tableUtils.sql(s"SELECT * FROM ${groupByConf.metaData.outputTable}")
  }

  /**
   * Helper function to allow a user to execute a Join.
   *
   * @param joinConf          api.Join Chronon scala Join API object
   * @param endDate           str this represents the last date we will perform the Join for
   * @param stepDays          int this will determine how we chunk filling the missing partitions
   * @param tableUtils        TableUtils this will be used to perform ops against our data sources
   * @param constantsProvider ConstantsProvider must be set from the Scala side. Doing so from PySpark will not properly set it.
   * @return DataFrame
   */
  def runJoin(joinConf: api.Join,
              endDate: String,
              stepDays: Option[Int],
              skipFirstHole: Boolean,
              sampleNumOfRows: Option[Int],
              tableUtils: BaseTableUtils,
              constantsProvider: ConstantNameProvider
             ): DataFrame = {
    logger.info(s"Executing Join ${joinConf.metaData.name}")
    Constants.initConstantNameProvider(constantsProvider)
    val join = new Join(
      joinConf,
      endDate,
      tableUtils,
      skipFirstHole = skipFirstHole
    )
    val resultDf = join.computeJoin(stepDays)
    logger.info(s"Finished executing Join ${joinConf.metaData.name}")
    resultDf
  }

  /**
   * Helper function to validate a GroupBy
   *
   * @param groupByConf       api.GroupBy Chronon scala GroupBy API object
   * @param startDate         start date for the group by
   * @param endDate           end date for the group by
   * @param tableUtils        TableUtils this will be used to perform ops against our data sources
   * @param constantsProvider ConstantsProvider must be set from the Scala side. Doing so from PySpark will not properly set it.
   * @return DataFrame
   */
  def validateGroupBy(groupByConf: api.GroupBy, startDate: String, endDate: String, tableUtils: BaseTableUtils, constantsProvider: ConstantNameProvider): List[String] = {
    logger.info(s"Validating GroupBy ${groupByConf.metaData.name}")
    Constants.initConstantNameProvider(constantsProvider)
    val validator = new Validator(tableUtils, groupByConf, startDate, endDate)
    val result = validator.validateGroupBy(groupByConf)
    logger.info(s"Finished validating GroupBy ${groupByConf.metaData.name}")
    result
  }


  /**
   * Helper function to validate a Join
   *
   * @param joinConf          api.Join Chronon scala Join API object
   * @param startDate         start date for the join
   * @param endDate           end date for the join
   * @param tableUtils        TableUtils this will be used to perform ops against our data sources
   * @param constantsProvider ConstantsProvider must be set from the Scala side. Doing so from PySpark will not properly set it.
   * @return DataFrame
   */
  def validateJoin(joinConf: api.Join, startDate: String, endDate: String, tableUtils: BaseTableUtils, constantsProvider: ConstantNameProvider): List[String] = {
    logger.info(s"Validating Join: ${joinConf.metaData.name}")
    Constants.initConstantNameProvider(constantsProvider)
    val validator = new Validator(tableUtils, joinConf, startDate, endDate)
    val result = validator.validateJoin(joinConf)
    logger.info(s"Finished validating Join: ${joinConf.metaData.name}")
    result
  }

  /**
   * Helper function to analyze a GroupBy
   *
   * @param groupByConf          api.GroupBy Chronon scala GroupBy API object
   * @param startDate            start date for the group by
   * @param endDate              end date for the group by
   * @param enableHitterAnalysis if true we will perform an analysis of what hot keys may be present
   * @param tableUtils           TableUtils this will be used to perform ops against our data sources
   * @param constantsProvider    ConstantsProvider must be set from the Scala side. Doing so from PySpark will not properly set it.
   */
  def analyzeGroupBy(groupByConf: api.GroupBy, startDate: String, endDate: String, enableHitterAnalysis: Boolean, tableUtils: BaseTableUtils, constantsProvider: ConstantNameProvider): Unit = {
    logger.info(s"Analyzing GroupBy: ${groupByConf.metaData.name}")
    Constants.initConstantNameProvider(constantsProvider)
    val analyzer = new Analyzer(tableUtils, groupByConf, startDate, endDate, enableHitter = enableHitterAnalysis)
    analyzer.analyzeGroupBy(groupByConf, enableHitter = enableHitterAnalysis)
    logger.info(s"Finished analyzing GroupBy: ${groupByConf.metaData.name}")
  }


  /**
   * Helper function to analyze a Join
   *
   * @param joinConf             api.Join Chronon scala Join API object
   * @param startDate            start date for the join
   * @param endDate              end date for the join
   * @param enableHitterAnalysis if true we will perform an analysis of what hot keys may be present
   * @param tableUtils           TableUtils this will be used to perform ops against our data sources
   * @param constantsProvider    ConstantsProvider must be set from the Scala side. Doing so from PySpark will not properly set it.
   * @return DataFrame
   */
  def analyzeJoin(joinConf: api.Join, startDate: String, endDate: String, enableHitterAnalysis: Boolean, tableUtils: BaseTableUtils, constantsProvider: ConstantNameProvider): Unit = {
    logger.info(s"Analyzing Join: ${joinConf.metaData.name}")
    Constants.initConstantNameProvider(constantsProvider)
    val analyzer = new Analyzer(tableUtils, joinConf, startDate, endDate, enableHitter = enableHitterAnalysis)
    analyzer.analyzeJoin(joinConf, enableHitter = enableHitterAnalysis)
    logger.info(s"Finished analyzing Join: ${joinConf.metaData.name}")
  }

}
