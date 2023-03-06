package ai.chronon.spark
import ai.chronon.api

import scala.util.Either


object SqlGenerator {

  def generateGroupBySql(groupBy: GroupBy) = {
    println("Generating GroupBy SQL for")
  }

  def generateJoinSql(join: Join) = {
    println("Generating Join SQL")
  }

  def runGroupBy(groupBy: api.GroupBy, ds: String, samplingRate: Option[Float], samplingMap: Option[Map[String, String]]): Unit = {
    println("Generating GroupBy SQL")
    println(ds)
    println(samplingRate)
    println(samplingMap)
  }

  def runJoin(join: api.Join, ds: String, samplingRate: Option[Float], samplingMap: Option[Map[String, String]]): Unit = {
    println("Generating Join SQL")
    println(ds)
    println(samplingRate)
    println(samplingMap)
  }

}
