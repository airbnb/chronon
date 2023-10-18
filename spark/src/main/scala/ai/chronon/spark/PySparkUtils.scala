package ai.chronon.spark
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, Resolution}
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api

import scala.collection.JavaConverters._

object PySparkUtils {


  
  /**
    * Pyspark has a tough time creating a FiveMinuteResolution via jvm.ai.chronon.aggregator.windowing.FiveMinuteResolution so we provide this helper method
    * @return FiveMinuteResolution
  */

  def getFiveMinuteResolution: Resolution = FiveMinuteResolution

  /**
    * Creating optionals is difficult to support in Pyspark, so we provide this method as a work around
    * @param timeRange a time range
    * @return Empty time range optional
  */
  def getTimeRangeOptional(timeRange: TimeRange): Option[TimeRange] = if (timeRange == null) Option.empty[TimeRange] else Option(timeRange)

  /**
    * Creating optionals is difficult to support in Pyspark, so we provide this method as a work around
    * @param str a string
    * @return String optional
  */
  def getStringOptional(str : String) : Option[String] = if (str == null) Option.empty[String] else Option(str)

  /**
    * Type parameters are difficult to support in Pyspark, so we provide these helper methods for ThriftJsonCodec.fromJsonStr
    * @param groupByJson a JSON string representing a group by
    * @return Chronon Scala API GroupBy object
  */
  def parseGroupBy(groupByJson: String): api.GroupBy = {
    ThriftJsonCodec.fromJsonStr[api.GroupBy](groupByJson, check =  true, classOf[api.GroupBy])
  }

  /**
    * Type parameters are difficult to support in Pyspark, so we provide these helper methods for ThriftJsonCodec.fromJsonStr
    * @param joinJson a JSON string representing a join
    * @return Chronon Scala API Join object
  */
  def parseJoin(joinJson: String): api.Join = {
    ThriftJsonCodec.fromJsonStr[api.Join](joinJson, check = true, classOf[api.Join])
  }

  /**
    * Type parameters are difficult to support in Pyspark, so we provide these helper methods for ThriftJsonCodec.fromJsonStr
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


}
