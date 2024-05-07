package ai.chronon.online

import ai.chronon.api.Extensions.{filePathOps}
import org.slf4j.LoggerFactory

case class MetadataEndPoint(extractFn: String => (Option[String], Option[String]), name: String)

object MetadataEndPoint {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  val ConfByKeyEndPoint = new MetadataEndPoint(
    extractFn = confPath => (Some(confPath.confPathToKey), confPath.confPathToOptConfStr),
    name = "ZIPLINE_METADATA"
  )

  val NameByTeamEndPoint = new MetadataEndPoint(
    extractFn = confPath => (confPath.confPathToTeamKey, Some(confPath.confPathToKey)),
    name = "ZIPLINE_METADATA_BY_TEAM"
  )

  val NameToEndPoint: Map[String, MetadataEndPoint] = Map(
    ConfByKeyEndPoint.name -> ConfByKeyEndPoint,
    NameByTeamEndPoint.name -> NameByTeamEndPoint
  )
}
