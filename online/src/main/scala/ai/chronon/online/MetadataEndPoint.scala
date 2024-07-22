package ai.chronon.online

import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.{GroupBy, Join, StagingQuery, ThriftJsonCodec, MetaData}
import org.apache.thrift.TBase
import org.slf4j.LoggerFactory
import org.json4s.jackson.JsonMethods._
import org.json4s._

import scala.reflect.ClassTag

case class MetadataEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](
    extractFn: (String, Conf) => (String, String),
    dataset: String
)
object MetadataEndPoint {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  val ConfByKeyEndPointName = "ZIPLINE_METADATA"
  val NameByTeamEndPointName = "CHRONON_ENTITY_BY_TEAM"
  val ActiveEntityListEndPointName = "CHRONON_ACTIVE_ENTITY_LIST"

  private def getTeamFromMetadata(metaData: MetaData): String = {
    val team = metaData.team
    if (metaData.customJson != null && metaData.customJson.nonEmpty) {
      implicit val formats = DefaultFormats
      val customJson = parse(metaData.customJson)
      val teamFromJson: String = (customJson \ "team_override").extractOpt[String].getOrElse("")
      if (teamFromJson.nonEmpty) teamFromJson else team
    } else team
  }

  private def parseTeam[Conf <: TBase[_, _]: Manifest: ClassTag](conf: Conf): String = {
    conf match {
      case join: Join                 => "joins/" + getTeamFromMetadata(join.metaData)
      case groupBy: GroupBy           => "group_bys/" + getTeamFromMetadata(groupBy.metaData)
      case stagingQuery: StagingQuery => "staging_queries/" + getTeamFromMetadata(stagingQuery.metaData)
      case _ =>
        logger.error(s"Failed to parse team from $conf")
        throw new Exception(s"Failed to parse team from $conf")
    }
  }

  private def parseEntityType[Conf <: TBase[_, _]: Manifest: ClassTag](conf: Conf): String = {
    conf match {
      case join: Join                 => "joins"
      case groupBy: GroupBy           => "group_bys"
      case stagingQuery: StagingQuery => "staging_queries"
      case _ =>
        logger.error(s"Failed to parse entity type from $conf")
        throw new Exception(s"Failed to parse entity type from $conf")
    }
  }

  // key: entity path, e.g. joins/team/team.example_join.v1
  // value: entity config in json format
  private def confByKeyEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (path, conf) => (path.confPathToKey, ThriftJsonCodec.toJsonStr(conf)),
      dataset = ConfByKeyEndPointName
    )

  // key: entity type + team name, e.g. joins/team
  // value: list of entities under the team, e.g. joins/team/team.example_join.v1, joins/team/team.example_join.v2
  private def NameByTeamEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (path, conf) => (parseTeam[Conf](conf), path.confPathToKey),
      dataset = NameByTeamEndPointName
    )

  // key: entity type, e.g. joins
  // value: list of entities of that entity type, e.g. joins/team/team.example_join.v1, joins/team/team.example_join.v2
  private def ActiveEntityListEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (path, conf) => (parseEntityType[Conf](conf), path.confPathToKey),
      dataset = NameByTeamEndPointName
    )

  def getEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](endPointName: String): MetadataEndPoint[Conf] = {
    endPointName match {
      case ConfByKeyEndPointName        => confByKeyEndPoint[Conf]
      case NameByTeamEndPointName       => NameByTeamEndPoint[Conf]
      case ActiveEntityListEndPointName => ActiveEntityListEndPoint[Conf]
      case _ =>
        logger.error(s"Failed to find endpoint for $endPointName")
        throw new Exception(s"Failed to find endpoint for $endPointName")
    }
  }

  def getEndPointDatasetName(endPointName: String): String = {
    endPointName match {
      case ConfByKeyEndPointName        => ConfByKeyEndPointName
      case NameByTeamEndPointName       => NameByTeamEndPointName
      case ActiveEntityListEndPointName => NameByTeamEndPointName
      case _ =>
        logger.error(s"Failed to find endpoint for $endPointName")
        throw new Exception(s"Failed to find endpoint for $endPointName")
    }
  }
}
