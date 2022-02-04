package ai.zipline.spark

import ai.zipline.api
import ai.zipline.api.Extensions.{GroupByOps, SourceOps}
import ai.zipline.api.{BooleanType, DoubleType, FloatType, IntType, LongType, ShortType, StructField, StructType, ThriftJsonCodec}
import ai.zipline.online.{Api, Fetcher, MetadataStore}
import com.google.gson.GsonBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.thrift.TBase
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}
import java.io.File

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader

// The mega zipline cli
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
        TableUtils(SparkSessionBuilder.build(s"join_${joinConf.metaData.name}")),
        args.skipEqualCheck()
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
    def serializableProps = {
      val map = new mutable.HashMap[String, String]()
      propsInner.foreach { case (key, value) => map.update(key, value) }
      map.toMap
    }

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
      val `type`: ScallopOption[String] = choice(Seq("join", "group-by"), descr = "the type of conf to fetch")
    }

    private def castValue(field: StructField, key: String, value: String): Any = {
      val casted = field.fieldType match {
        case IntType => value.toInt
        case LongType => value.toLong
        case DoubleType => value.toDouble
        case FloatType => value.toFloat
        case ShortType => value.toShort
        case BooleanType => value.toBoolean
        case _ => value
      }
      casted
    }

    def run(args: Args): Unit = {
      val gson = (new GsonBuilder()).setPrettyPrinting().create()
      val keyMap: Map[String, String] = gson.fromJson(args.keyJson(), classOf[java.util.Map[String, String]]).asScala.toMap

      val fetcher = new Fetcher(args.impl(args.serializableProps).genKvStore)

      val startNs = System.nanoTime
      val resultFuture = if (args.`type`() == "join") {
        val joinMetadata = fetcher.getJoinConf(args.name())
        val finalKeyMap: Map[String, AnyRef] = joinMetadata.joinPartOps.flatMap { part =>
          val groupByName = part.groupBy.getMetaData.getName
          val rightToLeftMap = part.rightToLeft
          fetcher.getGroupByServingInfo(groupByName).keyZiplineSchema.map {
            field =>
              val leftKey = rightToLeftMap(field.name)
              assert(keyMap.contains(leftKey), s"input key-json must contain field $leftKey")
              println(castValue(field, leftKey, keyMap(leftKey)).getClass)
              leftKey -> castValue(field, leftKey, keyMap(leftKey)).asInstanceOf[AnyRef]
          }
        }.toMap
        fetcher.fetchJoin(Seq(Fetcher.Request(args.name(), finalKeyMap)))
      } else {
        // group bys
        val groupByServingInfo = fetcher.getGroupByServingInfo(args.name())
        val finalKeyMap: Map[String, AnyRef] = groupByServingInfo.keyZiplineSchema.fields.map {
          field =>
            assert(keyMap.contains(field.name), s"input key-json must contain field ${field.name}")
            println(castValue(field, field.name, keyMap(field.name)).getClass)
            field.name -> castValue(field, field.name, keyMap(field.name)).asInstanceOf[AnyRef]
        }.toMap
        fetcher.fetchGroupBys(Seq(Fetcher.Request(args.name(), finalKeyMap)))
      }
      val result = Await.result(resultFuture, 1.minute)
      val awaitTimeMs = (System.nanoTime - startNs) / 1e6d

      // treeMap to produce a sorted result
      val tMap = new java.util.TreeMap[String, AnyRef]()
      println(result)
      result.foreach(r =>
        r.values match {
          case valMap: Map[String, AnyRef] => {
            valMap.foreach { case (k, v) => tMap.put(k, v) }
          }
          case _ => println(s"${r.request} has no values")
        })
      println(s"the returned values are: ${gson.toJson(tMap)}")
      println(s"Fetched in: $awaitTimeMs ms")
    }
  }

  object MetadataUploader {
    class Args extends Subcommand("metadata-upload") with OnlineSubcommand {
      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Path to the Zipline config file or directory")
    }

    def run(args: Args): Unit = {
      val metadataStore =
        new MetadataStore(args.impl(args.serializableProps).genKvStore, "ZIPLINE_METADATA", timeoutMillis = 10000)
      val putRequest = metadataStore.putConf(args.confPath())
      val res = Await.result(putRequest, 1.hour)
      println(
        s"Uploaded Zipline Configs to the KV store, success count = ${res.count(v => v)}, failure count = ${res.count(!_)}")
    }
  }

  object GroupByStreaming {
    def buildSession(local: Boolean): SparkSession = {
      val baseBuilder = SparkSession
        .builder()
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

    class Args extends Subcommand("group-by-streaming") with OnlineSubcommand {
      val confPath: ScallopOption[String] = opt[String](required = true, descr = "path to groupBy conf")
      val kafkaBootstrap: ScallopOption[String] =
        opt[String](required = true, descr = "host:port of a kafka bootstrap server")
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

    def run(args: Args): Unit = {
      val groupByConf: api.GroupBy = args.parseConf[api.GroupBy]
      val session: SparkSession = buildSession(args.debug())
      session.sparkContext.addJar(args.onlineJar())
      val streamingSource = groupByConf.streamingSource
      assert(streamingSource.isDefined, "There is no valid streaming source - with a valid topic, and endDate < today")
      val inputStream: DataFrame = dataStream(session, args.kafkaBootstrap(), streamingSource.get.topic)
      val streamingRunner =
        new streaming.GroupBy(inputStream, session, groupByConf, args.impl(args.serializableProps), args.debug())
      streamingRunner.run(args.debug())
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
          case args.GroupByStreamingArgs => {
            shouldExit = false
            GroupByStreaming.run(args.GroupByStreamingArgs)
          }
          case args.MetadataUploaderArgs => MetadataUploader.run(args.MetadataUploaderArgs)
          case args.FetcherCliArgs       => FetcherCli.run(args.FetcherCliArgs)
          case _                         => println(s"Unknown subcommand: ${x}")
        }
      case None => println(s"specify a subcommand please")
    }
    if (shouldExit) {
      System.exit(0)
    }
  }
}
