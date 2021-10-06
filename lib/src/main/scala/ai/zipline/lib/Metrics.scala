package ai.zipline.lib

import scala.collection.mutable

import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClient}

object Metrics {

  val statsd: StatsDClient = new NonBlockingStatsDClient("zipline", "localhost", 8125)

  object Tag {

    val GroupBy = "group_by"
    val Join = "join"
    val Streaming = "streaming"
    val Batch = "batch"
    val RequestType = "request_type"
    val Owner = "owner"
    val Method = "method"
    val Production = "production"
    val Accuracy = "accuracy"
    val Team = "team"
    val Exception = "exception"
  }
  case class Context(join: String = null,
                     groupBy: String = null,
                     production: Boolean = false,
                     isStaging: Boolean = false,
                     isStreaming: Boolean = false,
                     method: String = null,
                     owner: String = null,
                     accuracy: String = null,
                     team: String = null) {
    def withJoin(join: String): Context = copy(join = join)
    def withGroupBy(groupBy: String): Context = copy(groupBy = groupBy)
    def withIsStreaming(isStreaming: Boolean):  Context = copy(isStreaming = isStreaming)
    def withMethod(method: String): Context = copy(method = method)
    def withProduction(production: Boolean): Context = copy(production = production)
    def asBatch: Context = copy(isStreaming = false)
    def asStreaming: Context = copy(isStreaming = true)
    def withAccuracy(accuracy: String): Context = copy(accuracy = accuracy)
    def withTeam(team: String): Context = copy(team = team)

    def toTags(tags: (String, Any)*): Seq[String] = {
      assert(join != null || groupBy != null,
        "Either Join, groupBy should be set.")
      val result = mutable. HashMap.empty[String, String]
      if (join != null) {
        result.update(Tag.Join, join)
      }
      if (groupBy != null) {
        result.update(Tag.GroupBy, groupBy)
        result.update(Tag.Production, production.toString)
      }
      result.update(Tag.Team, team)
      result.update(Tag.Method, method)
      result.update(Tag.RequestType, requestType(isStreaming))
      result.toMap.map(t => s"${t._1}:${t._2}").toSeq ++
        tags.map(t => s"${t._1}:${t._2}")
    }

    private def requestType(isStreaming: Boolean): String = if (isStreaming) Tag.Streaming else Tag.Batch
  }
}
