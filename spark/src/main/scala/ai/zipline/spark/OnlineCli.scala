package ai.zipline.spark

import ai.zipline.api
import ai.zipline.api.Extensions.{GroupByOps, SourceOps}
import ai.zipline.api.ThriftJsonCodec
import ai.zipline.online.{Api, Fetcher, MetadataStore}
import com.google.gson.GsonBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.thrift.TBase
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag

// CLI for fetching, metadataupload
object OnlineCli {

  object FetcherCli {

    class Args extends Subcommand("fetch") {
      val keyJson: ScallopOption[String] = opt[String](required = true, descr = "json of the keys to fetch")
      val name: ScallopOption[String] = opt[String](required = true, descr = "name of the join/group-by to fetch")
      val `type`: ScallopOption[String] = choice(Seq("join", "group-by"), descr = "the type of conf to fetch")
    }

    def run(args: Args, onlineImpl: Api): Unit = {
      val gson = (new GsonBuilder()).setPrettyPrinting().create()
      val keyMap = gson.fromJson(args.keyJson(), classOf[java.util.Map[String, AnyRef]]).asScala.toMap

      val fetcher = new Fetcher(onlineImpl.genKvStore)
      val startNs = System.nanoTime
      val requests = Seq(Fetcher.Request(args.name(), keyMap))
      val resultFuture = if (args.`type`() == "join") {
        fetcher.fetchJoin(requests)
      } else {
        fetcher.fetchGroupBys(requests)
      }
      val result = Await.result(resultFuture, 1.minute)
      val awaitTimeMs = (System.nanoTime - startNs) / 1e6d

      // treeMap to produce a sorted result
      val tMap = new java.util.TreeMap[String, AnyRef]()
      result.head.values.foreach { case (k, v) => tMap.put(k, v) }
      println(gson.toJson(tMap))
      println(s"Fetched in: $awaitTimeMs ms")
    }
  }

  object MetadataUploader {
    class Args extends Subcommand("upload-metadata") {
      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Path to the Zipline config file or directory")
    }

    def run(args: Args, onlineImpl: Api): Unit = {
      val metadataStore = new MetadataStore(onlineImpl.genKvStore, "ZIPLINE_METADATA", timeoutMillis = 10000)
      val putRequest = metadataStore.putConf(args.confPath())
      val res = Await.result(putRequest, 1.hour)
      println(
        s"Uploaded Zipline Configs to the KV store, success count = ${res.count(v => v)}, failure count = ${res.count(!_)}")
    }
  }

  object GroupByStreaming {
    def buildSession(appName: String, local: Boolean): SparkSession = {
      val baseBuilder = SparkSession
        .builder()
        .appName(appName)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "ai.zipline.spark.ZiplineKryoRegistrator")
        .config("spark.kryoserializer.buffer.max", "2000m")
        .config("spark.kryo.referenceTracking", "false")

      val builder = if (local) {
        baseBuilder
        // use all threads - or the tests will be slow
          .master("local[*]")
          .config("spark.local.dir", s"/tmp/zipline-spark-streaming")
          .config("spark.kryo.registrationRequired", "true")
      } else {
        baseBuilder
      }
      builder.getOrCreate()
    }

    def dataStream(session: SparkSession, host: String, topic: String): DataFrame = {
      session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("enable.auto.commit", "true")
        .load()
        .selectExpr("value")
    }

    class Args extends Subcommand("stream-group-by") {
      val confPath: ScallopOption[String] = opt[String](required = true, descr = "path to groupBy conf")
      val kafkaBootstrap: ScallopOption[String] =
        opt[String](required = true, descr = "host:port of a kafka bootstrap server")
      val mockWrites: ScallopOption[Boolean] = opt[Boolean](required = false,
                                                            default = Some(false),
                                                            descr =
                                                              "flag - to ignore writing to the underlying kv store")
      val debug: ScallopOption[Boolean] = opt[Boolean](required = false,
                                                       default = Some(false),
                                                       descr =
                                                         "Prints details of data flowing through the streaming job")
      val local: ScallopOption[Boolean] =
        opt[Boolean](required = false, default = Some(false), descr = "Launches the job locally")
      def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
        ThriftJsonCodec.fromJsonFile[T](confPath(), check = true)
    }

    def run(args: Args, onlineImpl: Api): Unit = {
      val groupByConf: api.GroupBy = args.parseConf[api.GroupBy]
      val session: SparkSession = buildSession(groupByConf.metaData.name, args.local())
      val streamingSource = groupByConf.streamingSource
      assert(streamingSource.isDefined, "There is no valid streaming source - with a valid topic, and endDate < today")
      val inputStream: DataFrame = dataStream(session, args.kafkaBootstrap(), streamingSource.get.topic)
      val streamingRunner =
        new streaming.GroupBy(inputStream, session, groupByConf, onlineImpl, args.debug(), args.mockWrites())
      streamingRunner.run()
    }
  }

  class Args(args: Array[String]) extends ScallopConf(args) {
    val userConf: Map[String, String] = props[String]('D')
    val onlineClass: ScallopOption[String] =
      opt[String](descr = "Fully qualified OnlineImpl class. We expect the jar to be on the class path")
    object FetcherCliArgs extends FetcherCli.Args
    addSubcommand(FetcherCliArgs)
    object MetadataUploaderArgs extends MetadataUploader.Args
    addSubcommand(MetadataUploaderArgs)
    object GroupByStreamingArgs extends GroupByStreaming.Args
    addSubcommand(GroupByStreamingArgs)
    requireSubcommand()
    verify()
  }

  def onlineBuilder(userConf: Map[String, String], onlineClass: String): Api = {
    val cls = Class.forName(onlineClass)
    val constructor = cls.getConstructor(classOf[Map[String, String]])
    val onlineImpl = constructor.newInstance(userConf)
    onlineImpl.asInstanceOf[Api]
  }

  def main(baseArgs: Array[String]): Unit = {
    val args = new Args(baseArgs)
    val onlineImpl = onlineBuilder(args.userConf, args.onlineClass())
    args.subcommand match {
      case Some(x) =>
        x match {
          case args.FetcherCliArgs       => FetcherCli.run(args.FetcherCliArgs, onlineImpl)
          case args.MetadataUploaderArgs => MetadataUploader.run(args.MetadataUploaderArgs, onlineImpl)
          case args.GroupByStreamingArgs => GroupByStreaming.run(args.GroupByStreamingArgs, onlineImpl)
          case _                         => println(s"Unknown subcommand: ${x}")
        }
      case None => println(s"specify a subcommand please")
    }
  }
}
