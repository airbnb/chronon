package ai.chronon.online

import ai.chronon.api.Extensions.{filePathOps}
import org.slf4j.LoggerFactory

case class MetadataEndPoint(extractFn: String => (Option[String], Option[String]), name: String)

object MetadataEndPoint {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  // key: entity path, e.g. joins/team/team.example_join.v1
  // value: entity config in json format
  val ConfByKeyEndPoint = new MetadataEndPoint(
    extractFn = confPath => (Some(confPath.confPathToKey), confPath.confPathToOptConfStr),
    name = "ZIPLINE_METADATA"
  )

  // key: entity type + team name, e.g. joins/team
  // value: list of entities under the team, e.g. joins/team/team.example_join.v1, joins/team/team.example_join.v2
  val NameByTeamEndPoint = new MetadataEndPoint(
    extractFn = confPath => (confPath.confPathToTeamKey, Some(confPath.confPathToKey)),
    name = "ZIPLINE_METADATA_BY_TEAM"
  )

  val NameToEndPoint: Map[String, MetadataEndPoint] = Map(
    ConfByKeyEndPoint.name -> ConfByKeyEndPoint,
    NameByTeamEndPoint.name -> NameByTeamEndPoint
  )
}
