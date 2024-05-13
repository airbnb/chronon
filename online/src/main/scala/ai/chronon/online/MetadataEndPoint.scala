package ai.chronon.online

import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.{GroupBy, Join, StagingQuery, ThriftJsonCodec}
import org.apache.thrift.TBase
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

case class MetadataEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](
    extractFn: (String, Conf) => (String, String),
    name: String
)
object MetadataEndPoint {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  val ConfByKeyEndPointName = "ZIPLINE_METADATA"
  val NameByTeamEndPointName = "ZIPLINE_METADATA_BY_TEAM"

  private def parseTeam[Conf <: TBase[_, _]: Manifest: ClassTag](conf: Conf): String = {
    conf match {
      case join: Join                 => "joins/" + join.metaData.team
      case groupBy: GroupBy           => "group_bys/" + groupBy.metaData.team
      case stagingQuery: StagingQuery => "staging_queries/" + stagingQuery.metaData.team
      case _ =>
        logger.error(s"Failed to parse team from $conf")
        throw new Exception(s"Failed to parse team from $conf")
    }
  }

  // key: entity path, e.g. joins/team/team.example_join.v1
  // value: entity config in json format
  private def confByKeyEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (path, conf) => (path.confPathToKey, ThriftJsonCodec.toJsonStr(conf)),
      name = ConfByKeyEndPointName
    )

  // key: entity type + team name, e.g. joins/team
  // value: list of entities under the team, e.g. joins/team/team.example_join.v1, joins/team/team.example_join.v2
  private def NameByTeamEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (path, conf) => (parseTeam[Conf](conf), path.confPathToKey),
      name = NameByTeamEndPointName
    )

  def getEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](endPointName: String): MetadataEndPoint[Conf] = {
    endPointName match {
      case ConfByKeyEndPointName  => confByKeyEndPoint[Conf]
      case NameByTeamEndPointName => NameByTeamEndPoint[Conf]
      case _ =>
        logger.error(s"Failed to find endpoint for $endPointName")
        throw new Exception(s"Failed to find endpoint for $endPointName")
    }
  }
}
