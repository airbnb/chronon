package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.online.{Api, Fetcher, MetadataStore}
import ai.chronon.spark.consistency.ConsistencyJob
import ai.chronon.spark.streaming.TopicChecker
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}
import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}
import org.apache.thrift.TBase
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success}

// useful to override spark.sql.extensions args - there is no good way to unset that conf apparently
// so we give it dummy extensions
class DummyExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {}
}

// The mega chronon cli
object Driver {

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = true)

  trait OfflineSubcommand { this: ScallopConf =>
    val confPath: ScallopOption[String] = opt[String](required = true, descr = "Path to conf")
    val endDate: ScallopOption[String] =
      opt[String](required = false, descr = "End date to compute as of, start date is taken from conf.")
  }

  object JoinBackfill {
    class Args extends Subcommand("join") with OfflineSubcommand {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs backfill in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
      val skipEqualCheck: ScallopOption[Boolean] =
        opt[Boolean](required = false,
                     default = Some(false),
                     descr = "Check if this join has already run with a different conf, if so it will fail the job")
    }

    def run(args: Args): Unit = {
      val joinConf = parseConf[api.Join](args.confPath())
      val join = new Join(
        joinConf,
        args.endDate(),
        TableUtils(SparkSessionBuilder.build(s"join_${joinConf.metaData.name}"))
      )
      join.computeJoin(args.stepDays.toOption)
    }
  }

  object GroupByBackfill {
    class Args extends Subcommand("group-by-backfill") with OfflineSubcommand {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs backfill in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
    }
    def run(args: Args): Unit = {
      val groupByConf = parseConf[api.GroupBy](args.confPath())
      GroupBy.computeBackfill(
        groupByConf,
        args.endDate(),
        TableUtils(SparkSessionBuilder.build(s"groupBy_${groupByConf.metaData.name}_backfill")),
        args.stepDays.toOption
      )
    }
  }

  object StagingQueryBackfill {
    class Args extends Subcommand("staging-query-backfill") with OfflineSubcommand {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs backfill in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
    }
    def run(args: Args): Unit = {
      val stagingQueryConf = parseConf[api.StagingQuery](args.confPath())
      val stagingQueryJob = new StagingQuery(
        stagingQueryConf,
        args.endDate(),
        TableUtils(SparkSessionBuilder.build(s"staging_query_${stagingQueryConf.metaData.name}_backfill"))
      )
      stagingQueryJob.computeStagingQuery(args.stepDays.toOption)
    }
  }

  object GroupByUploader {
    class Args extends Subcommand("group-by-upload") with OfflineSubcommand {}

    def run(args: Args): Unit = {
      GroupByUpload.run(parseConf[api.GroupBy](args.confPath()), args.endDate())
    }
  }

  // common arguments to all online commands
  trait OnlineSubcommand { s: ScallopConf =>
    // this is `-Z` and not `-D` because sbt-pack plugin uses that for JAVA_OPTS
    val propsInner: Map[String, String] = props[String]('Z')
    val onlineJar: ScallopOption[String] =
      opt[String](required = true, descr = "Path to the jar contain the implementation of Online.Api class")
    val onlineClass: ScallopOption[String] =
      opt[String](required = true,
                  descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")

    // hashmap implements serializable
    def serializableProps: Map[String, String] = {
      val map = new mutable.HashMap[String, String]()
      propsInner.foreach { case (key, value) => map.update(key, value) }
      map.toMap
    }

    def metaDataStore =
      new MetadataStore(impl(serializableProps).genKvStore, "ZIPLINE_METADATA", timeoutMillis = 10000)

    def impl(props: Map[String, String]): Api = {
      val urls = Array(new File(onlineJar()).toURI.toURL)
      val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
      val cls = cl.loadClass(onlineClass())
      val constructor = cls.getConstructors.apply(0)
      val onlineImpl = constructor.newInstance(props)
      onlineImpl.asInstanceOf[Api]
    }
  }

  object FetcherCli {

    class Args extends Subcommand("fetch") with OnlineSubcommand {
      val keyJson: ScallopOption[String] = opt[String](required = true, descr = "json of the keys to fetch")
      val name: ScallopOption[String] = opt[String](required = true, descr = "name of the join/group-by to fetch")
      val `type`: ScallopOption[String] =
        choice(Seq("join", "group-by"), descr = "the type of conf to fetch", default = Some("join"))
    }

    def run(args: Args): Unit = {
      val objectMapper = new ObjectMapper()
      val keyMap = objectMapper.readValue(args.keyJson(), classOf[java.util.Map[String, AnyRef]]).asScala.toMap
      val fetcher = args.impl(args.serializableProps).buildFetcher(true)
      val startNs = System.nanoTime
      val requests = Seq(Fetcher.Request(args.name(), keyMap))
      val resultFuture = if (args.`type`() == "join") {
        fetcher.fetchJoin(requests)
      } else {
        fetcher.fetchGroupBys(requests)
      }
      val result = Await.result(resultFuture, 5.seconds)
      val awaitTimeMs = (System.nanoTime - startNs) / 1e6d

      // treeMap to produce a sorted result
      val tMap = new java.util.TreeMap[String, AnyRef]()
      result.foreach(r =>
        r.values match {
          case Success(valMap)    => valMap.foreach { case (k, v) => tMap.put(k, v) }
          case Failure(exception) => throw exception
        })
      println(s"--- [FETCHED RESULT] ---\n${objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(tMap)}")
      println(s"Fetched in: $awaitTimeMs ms")
    }
  }

  object MetadataUploader {
    class Args extends Subcommand("metadata-upload") with OnlineSubcommand {
      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Path to the Chronon config file or directory")
    }

    def run(args: Args): Unit = {
      val putRequest = args.metaDataStore.putConf(args.confPath())
      val res = Await.result(putRequest, 1.hour)
      println(
        s"Uploaded Chronon Configs to the KV store, success count = ${res.count(v => v)}, failure count = ${res.count(!_)}")
    }
  }

  object ConsistencyMetricsUploader {
    class Args extends Subcommand("consistency-metrics-upload") with OnlineSubcommand {
      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Path to the Chronon join conf file to compute consistency for")
      val endDate: ScallopOption[String] =
        opt[String](required = false, descr = "End date to compute metrics until.")
    }

    /**
      * class ConsistencyJob(session: SparkSession,
                     joinConf: api.Join,
                     endDate: String,
                     joinCodec: JoinCodec,
                     rawTable: String) {
      * */
    def run(args: Args): Unit = {
      val apiImpl = args.impl(args.serializableProps)
      val joinConf = parseConf[api.Join](args.confPath())
      new ConsistencyJob(
        SparkSessionBuilder.build(s"consistency_metrics_join_${joinConf.metaData.name}"),
        joinConf,
        args.endDate(),
        apiImpl
      ).buildConsistencyMetrics()
    }
  }

  object GroupByStreaming {
    def dataStream(session: SparkSession, host: String, topic: String): DataFrame = {
      TopicChecker.topicShouldExist(topic, host)
      session.streams.addListener(new StreamingQueryListener() {
        override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
          println("Query started: " + queryStarted.id)
        }
        override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
          println("Query terminated: " + queryTerminated.id)
        }
        override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
          println("Query made progress: " + queryProgress.progress)
        }
      })
      session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("enable.auto.commit", "true")
        .load()
        .selectExpr("value")
    }

    class Args extends Subcommand("group-by-streaming") with OnlineSubcommand {
      val confPath: ScallopOption[String] = opt[String](required = true, descr = "path to groupBy conf")
      val kafkaBootstrap: ScallopOption[String] =
        opt[String](required = false, descr = "host:port of a kafka bootstrap server")
      val mockWrites: ScallopOption[Boolean] = opt[Boolean](required = false,
                                                            default = Some(false),
                                                            descr =
                                                              "flag - to ignore writing to the underlying kv store")
      val debug: ScallopOption[Boolean] = opt[Boolean](
        required = false,
        default = Some(false),
        descr = "Prints details of data flowing through the streaming job, skip writing to kv store")
      def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
        ThriftJsonCodec.fromJsonFile[T](confPath(), check = true)
    }

    def findFile(path: String): Option[String] = {
      val tail = path.split("/").last
      val possiblePaths = Seq(path, tail, SparkFiles.get(tail))
      val statuses = possiblePaths.map(p => p -> new File(p).exists())

      val messages = statuses.map {
        case (file, present) =>
          val suffix = if (present) {
            val fileSize = Files.size(Paths.get(file))
            s"exists ${FileUtils.byteCountToDisplaySize(fileSize)}"
          } else {
            "is not found"
          }
          s"$file $suffix"
      }
      println(s"File Statuses:\n  ${messages.mkString("\n  ")}")
      statuses.find(_._2 == true).map(_._1)
    }

    def run(args: Args): Unit = {
      // session needs to be initialized before we can call find file.
      val session: SparkSession = SparkSessionBuilder.buildStreaming(args.debug())

      val confFile = findFile(args.confPath())
      val groupByConf = confFile
        .map(ThriftJsonCodec.fromJsonFile[api.GroupBy](_, check = false))
        .getOrElse(args.metaDataStore.getConf[api.GroupBy](args.confPath()).get)

      val onlineJar = findFile(args.onlineJar())
      if (args.debug())
        onlineJar.foreach(session.sparkContext.addJar)
      val streamingSource = groupByConf.streamingSource
      assert(streamingSource.isDefined, "There is no valid streaming source - with a valid topic, and endDate < today")
      lazy val host = streamingSource.get.topicTokens.get("host")
      lazy val port = streamingSource.get.topicTokens.get("port")
      if (!args.kafkaBootstrap.isDefined)
        assert(
          host.isDefined && port.isDefined,
          "Either specify a kafkaBootstrap url or provide host and port in your topic definition as topic/host=host/port=port")
      val inputStream: DataFrame =
        dataStream(session, args.kafkaBootstrap.getOrElse(s"${host.get}:${port.get}"), streamingSource.get.cleanTopic)
      val streamingRunner =
        new streaming.GroupBy(inputStream, session, groupByConf, args.impl(args.serializableProps), args.debug())
      val query = streamingRunner.run(args.debug())
      query.awaitTermination()
    }
  }

  class Args(args: Array[String]) extends ScallopConf(args) {
    object JoinBackFillArgs extends JoinBackfill.Args
    addSubcommand(JoinBackFillArgs)
    object GroupByBackfillArgs extends GroupByBackfill.Args
    addSubcommand(GroupByBackfillArgs)
    object StagingQueryBackfillArgs extends StagingQueryBackfill.Args
    addSubcommand(StagingQueryBackfillArgs)
    object GroupByUploadArgs extends GroupByUploader.Args
    addSubcommand(GroupByUploadArgs)
    object FetcherCliArgs extends FetcherCli.Args
    addSubcommand(FetcherCliArgs)
    object MetadataUploaderArgs extends MetadataUploader.Args
    addSubcommand(MetadataUploaderArgs)
    object GroupByStreamingArgs extends GroupByStreaming.Args
    addSubcommand(GroupByStreamingArgs)
    requireSubcommand()
    verify()
  }

  def onlineBuilder(userConf: Map[String, String], onlineJar: String, onlineClass: String): Api = {
    val urls = Array(new File(onlineJar).toURI.toURL)
    val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(userConf)
    onlineImpl.asInstanceOf[Api]
  }

  def main(baseArgs: Array[String]): Unit = {
    val args = new Args(baseArgs)
    var shouldExit = true
    args.subcommand match {
      case Some(x) =>
        x match {
          case args.JoinBackFillArgs         => JoinBackfill.run(args.JoinBackFillArgs)
          case args.GroupByBackfillArgs      => GroupByBackfill.run(args.GroupByBackfillArgs)
          case args.StagingQueryBackfillArgs => StagingQueryBackfill.run(args.StagingQueryBackfillArgs)
          case args.GroupByUploadArgs        => GroupByUploader.run(args.GroupByUploadArgs)
          case args.GroupByStreamingArgs =>
            shouldExit = false
            GroupByStreaming.run(args.GroupByStreamingArgs)

          case args.MetadataUploaderArgs => MetadataUploader.run(args.MetadataUploaderArgs)
          case args.FetcherCliArgs       => FetcherCli.run(args.FetcherCliArgs)
          case _                         => println(s"Unknown subcommand: $x")
        }
      case None => println(s"specify a subcommand please")
    }
    if (shouldExit) {
      System.exit(0)
    }
  }
}
