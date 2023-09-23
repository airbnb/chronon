package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.online.{Api, Fetcher, MetadataStore}
import ai.chronon.spark.stats.{CompareBaseJob, CompareJob, ConsistencyJob, SummaryJob}
import ai.chronon.spark.streaming.{JoinSourceRunner, TopicChecker}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
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
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success, Try}

// useful to override spark.sql.extensions args - there is no good way to unset that conf apparently
// so we give it dummy extensions
class DummyExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {}
}

// The mega chronon cli
object Driver {

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = true)

  trait OfflineSubcommand {
    this: ScallopConf =>
    val confPath: ScallopOption[String] = opt[String](required = true, descr = "Path to conf")

    private val endDateInternal: ScallopOption[String] =
      opt[String](name = "end-date",
                  required = false,
                  descr = "End date to compute as of, start date is taken from conf.")

    val localTableMapping: Map[String, String] = propsLong[String](
      name = "local-table-mapping",
      keyName = "namespace.table",
      valueName = "path_to_local_input_file",
      descr = """Use this option to specify a list of table <> local input file mappings for running local
          |Chronon jobs. For example,
          |`--local-data-list ns_1.table_a=p1/p2/ta.csv ns_2.table_b=p3/tb.csv`
          |will load the two files into the specified tables `table_a` and `table_b` locally.
          |Once this option is used, the `--local-data-path` will be ignored.
          |""".stripMargin
    )
    val localDataPath: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr =
          """Path to a folder containing csv data to load from. You can refer to these in Sources to run the backfill.
            |The name of each file should be in the format namespace.table.csv. They can be referred to in the confs
            |as "namespace.table". When namespace is not specified we will default to 'default'. We can also
            |auto-convert ts columns encoded as readable strings in the format 'yyyy-MM-dd HH:mm:ss'',
            |into longs values expected by Chronon automatically.
            |""".stripMargin
      )
    val localWarehouseLocation: ScallopOption[String] =
      opt[String](
        required = false,
        default = Some(System.getProperty("user.dir") + "/local_warehouse"),
        descr = "Directory to write locally loaded warehouse data into. This will contain unreadable parquet files"
      )

    lazy val sparkSession: SparkSession = buildSparkSession()

    def endDate(): String = endDateInternal.toOption.getOrElse(buildTableUtils().partitionSpec.now)

    def subcommandName(): String

    def isLocal: Boolean = localTableMapping.nonEmpty || localDataPath.isDefined

    protected def buildSparkSession(): SparkSession = {
      if (localTableMapping.nonEmpty) {
        val localSession = SparkSessionBuilder.build(subcommandName(), local = true, localWarehouseLocation.toOption)
        localTableMapping.foreach {
          case (table, filePath) =>
            val file = new File(filePath)
            LocalDataLoader.loadDataFileAsTable(file, localSession, table)
        }
        localSession
      } else if (localDataPath.isDefined) {
        val dir = new File(localDataPath())
        assert(dir.exists, s"Provided local data path: ${localDataPath()} doesn't exist")
        val localSession =
          SparkSessionBuilder.build(subcommandName(),
                                    local = true,
                                    localWarehouseLocation = localWarehouseLocation.toOption)
        LocalDataLoader.loadDataRecursively(dir, localSession)
        localSession
      } else {
        SparkSessionBuilder.build(subcommandName())
      }
    }

    def buildTableUtils(): TableUtils = {
      TableUtils(sparkSession)
    }
  }

  trait LocalExportTableAbility {
    this: ScallopConf with OfflineSubcommand =>

    val localTableExportPath: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr =
          """Path to a folder for exporting all the tables generated during the run. This is only effective when local
            |input data is used: when either `local-table-mapping` or `local-data-path` is set. The name of the file
            |will be of format [<prefix>].<namespace>.<table_name>.<format>. For example: "default.test_table.csv" or
            |"local_prefix.some_namespace.another_table.parquet".
            |""".stripMargin
      )

    val localTableExportFormat: ScallopOption[String] =
      opt[String](
        required = false,
        default = Some("csv"),
        validate = (format: String) => LocalTableExporter.SupportedExportFormat.contains(format.toLowerCase),
        descr = "The table output format, supports csv(default), parquet, json."
      )

    val localTableExportPrefix: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr = "The prefix to put in the exported file name."
      )

    protected def buildLocalTableExporter(tableUtils: TableUtils): LocalTableExporter =
      new LocalTableExporter(tableUtils,
                             localTableExportPath(),
                             localTableExportFormat(),
                             localTableExportPrefix.toOption)

    def shouldExport(): Boolean = isLocal && localTableExportPath.isDefined

    def exportTableToLocal(namespaceAndTable: String, tableUtils: TableUtils): Unit = {
      val isLocal = localTableMapping.nonEmpty || localDataPath.isDefined
      val shouldExport = localTableExportPath.isDefined
      if (!isLocal || !shouldExport) {
        return
      }

      buildLocalTableExporter(tableUtils).exportTable(namespaceAndTable)
    }
  }

  trait ResultValidationAbility {
    this: ScallopConf with OfflineSubcommand =>

    val expectedResultTable: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr = """The name of the table containing expected result of a job.
            |The table should have the exact schema of the output of the job""".stripMargin
      )

    def shouldPerformValidate(): Boolean = expectedResultTable.isDefined

    def validateResult(df: DataFrame, keys: Seq[String], tableUtils: TableUtils): Boolean = {
      val expectedDf = tableUtils.loadEntireTable(expectedResultTable())
      val (_, _, metrics) = CompareBaseJob.compare(df, expectedDf, keys, tableUtils)
      val result = CompareJob.getConsolidatedData(metrics, tableUtils.partitionSpec)

      if (result.nonEmpty) {
        println("[Validation] Failed. Please try exporting the result and investigate.")
        false
      } else {
        println("[Validation] Success.")
        true
      }
    }
  }

  object JoinBackfill {
    class Args
        extends Subcommand("join")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs backfill in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
      val runFirstHole: ScallopOption[Boolean] =
        opt[Boolean](required = false,
                     default = Some(false),
                     descr = "Skip the first unfilled partition range if some future partitions have been populated.")
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName() = s"join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val join = new Join(
        args.joinConf,
        args.endDate(),
        args.buildTableUtils(),
        !args.runFirstHole()
      )
      val df = join.computeJoin(args.stepDays.toOption)

      if (args.shouldExport()) {
        args.exportTableToLocal(args.joinConf.metaData.outputTable, tableUtils)
      }

      if (args.shouldPerformValidate()) {
        val keys = CompareJob.getJoinKeys(args.joinConf, tableUtils)
        args.validateResult(df, keys, tableUtils)
      }
    }
  }

  object GroupByBackfill {
    class Args
        extends Subcommand("group-by-backfill")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs backfill in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
      lazy val groupByConf: api.GroupBy = parseConf[api.GroupBy](confPath())
      override def subcommandName() = s"groupBy_${groupByConf.metaData.name}_backfill"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      GroupBy.computeBackfill(
        args.groupByConf,
        args.endDate(),
        tableUtils,
        args.stepDays.toOption
      )

      if (args.shouldExport()) {
        args.exportTableToLocal(args.groupByConf.metaData.outputTable, tableUtils)
      }

      if (args.shouldPerformValidate()) {
        val df = tableUtils.loadEntireTable(args.groupByConf.metaData.outputTable)
        args.validateResult(df, args.groupByConf.keys(tableUtils.partitionColumn).toSeq, tableUtils)
      }
    }
  }

  object LabelJoin {
    class Args extends Subcommand("label-join") with OfflineSubcommand with LocalExportTableAbility {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs label join in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName() = s"label_join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val labelJoin = new LabelJoin(
        args.joinConf,
        tableUtils,
        args.endDate()
      )
      labelJoin.computeLabelJoin(args.stepDays.toOption)

      if (args.shouldExport()) {
        args.exportTableToLocal(args.joinConf.metaData.outputLabelTable, tableUtils)
      }
    }
  }

  object Analyzer {
    class Args extends Subcommand("analyze") with OfflineSubcommand {
      val startDate: ScallopOption[String] =
        opt[String](required = false,
                    descr = "Finds heavy hitters & time-distributions until a specified start date",
                    default = None)
      val count: ScallopOption[Int] =
        opt[Int](
          required = false,
          descr =
            "Finds the specified number of heavy hitters approximately. The larger this number is the more accurate the analysis will be.",
          default = Option(128)
        )
      val sample: ScallopOption[Double] =
        opt[Double](required = false,
                    descr = "Sampling ratio - what fraction of rows into incorporate into the heavy hitter estimate",
                    default = Option(0.1))
      val enableHitter: ScallopOption[Boolean] =
        opt[Boolean](
          required = false,
          descr =
            "enable skewed data analysis - whether to include the heavy hitter analysis, will only output schema if disabled",
          default = Some(false)
        )

      override def subcommandName() = "analyzer_util"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      new Analyzer(tableUtils,
                   args.confPath(),
                   args.startDate.getOrElse(tableUtils.partitionSpec.shiftBackFromNow(3)),
                   args.endDate(),
                   args.count(),
                   args.sample(),
                   args.enableHitter()).run
    }
  }

  object MetadataExport {
    class Args extends Subcommand("metadata-export") with OfflineSubcommand {
      val inputRootPath: ScallopOption[String] =
        opt[String](required = true, descr = "Base path of config repo to export from")
      val outputRootPath: ScallopOption[String] =
        opt[String](required = true, descr = "Base path to write output metadata files to")
      override def subcommandName() = "metadata-export"
    }

    def run(args: Args): Unit = {
      MetadataExporter.run(args.inputRootPath(), args.outputRootPath())
    }
  }

  object StagingQueryBackfill {
    class Args extends Subcommand("staging-query-backfill") with OfflineSubcommand with LocalExportTableAbility {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs backfill in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
      val enableAutoExpand: ScallopOption[Boolean] =
        opt[Boolean](required = false,
                     descr = "Auto expand hive table if new columns added in staging query",
                     default = Option(true))
      lazy val stagingQueryConf: api.StagingQuery = parseConf[api.StagingQuery](confPath())
      override def subcommandName() = s"staging_query_${stagingQueryConf.metaData.name}_backfill"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val stagingQueryJob = new StagingQuery(
        args.stagingQueryConf,
        args.endDate(),
        tableUtils
      )
      stagingQueryJob.computeStagingQuery(args.stepDays.toOption, args.enableAutoExpand.toOption)

      if (args.shouldExport()) {
        args.exportTableToLocal(args.stagingQueryConf.metaData.outputTable, tableUtils)
      }
    }
  }

  object DailyStats {
    class Args extends Subcommand("stats-summary") with OfflineSubcommand {
      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs backfill in steps, step-days at a time. Default is 30 days",
                 default = Option(30))
      val sample: ScallopOption[Double] =
        opt[Double](required = false,
                    descr = "Sampling ratio - what fraction of rows into incorporate into the heavy hitter estimate",
                    default = Option(0.1))
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName() = s"daily_stats_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      new SummaryJob(args.sparkSession, args.joinConf, endDate = args.endDate())
        .dailyRun(Some(args.stepDays()), args.sample())
    }
  }

  object GroupByUploader {
    class Args extends Subcommand("group-by-upload") with OfflineSubcommand {
      override def subcommandName() = "group-by-upload"
    }

    def run(args: Args): Unit = {
      GroupByUpload.run(parseConf[api.GroupBy](args.confPath()), args.endDate())
    }
  }

  object ConsistencyMetricsCompute {
    class Args extends Subcommand("consistency-metrics-compute") with OfflineSubcommand {
      override def subcommandName() = "consistency-metrics-compute"
    }

    def run(args: Args): Unit = {
      val joinConf = parseConf[api.Join](args.confPath())
      new ConsistencyJob(
        args.sparkSession,
        joinConf,
        args.endDate()
      ).buildConsistencyMetrics()
    }
  }

  object CompareJoinQuery {
    class Args extends Subcommand("compare-join-query") with OfflineSubcommand {
      val queryConf: ScallopOption[String] =
        opt[String](required = true, descr = "Conf to the Staging Query to compare with")
      val startDate: ScallopOption[String] =
        opt[String](required = false, descr = "Partition start date to compare the data from")

      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      lazy val stagingQueryConf: api.StagingQuery = parseConf[api.StagingQuery](queryConf())
      override def subcommandName() = s"compare_join_query_${joinConf.metaData.name}_${stagingQueryConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      assert(args.confPath().contains("/joins/"), "Conf should refer to the join path")
      assert(args.queryConf().contains("/staging_queries/"), "Compare path should refer to the staging query path")

      val tableUtils = args.buildTableUtils()
      new CompareJob(
        tableUtils,
        args.joinConf,
        args.stagingQueryConf,
        args.startDate.getOrElse(tableUtils.partitionSpec.at(System.currentTimeMillis())),
        args.endDate()
      ).run()
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
      val keyJson: ScallopOption[String] = opt[String](required = false, descr = "json of the keys to fetch")
      val name: ScallopOption[String] = opt[String](required = true, descr = "name of the join/group-by to fetch")
      val `type`: ScallopOption[String] =
        choice(Seq("join", "group-by", "join-stats"), descr = "the type of conf to fetch", default = Some("join"))
      val keyJsonFile: ScallopOption[String] = opt[String](
        required = false,
        descr = "file path to json of the keys to fetch",
        short = 'f'
      )
      val interval: ScallopOption[Int] = opt[Int](
        required = false,
        descr = "interval between requests in seconds",
        default = Some(1)
      )
      val loop: ScallopOption[Boolean] = opt[Boolean](
        required = false,
        descr = "flag - loop over the requests until manually killed",
        default = Some(false)
      )
    }

    def fetchStats(args: Args, objectMapper: ObjectMapper, keyMap: Map[String, AnyRef], fetcher: Fetcher): Unit = {
      val resFuture = fetcher.fetchStatsTimeseries(
        Fetcher.StatsRequest(
          args.name(),
          keyMap.get("startTs").map(_.asInstanceOf[String].toLong),
          keyMap.get("endTs").map(_.asInstanceOf[String].toLong)
        ))
      val stats = Await.result(resFuture, 100.seconds)
      val series = stats.values.get
      val toPrint =
        if (
          keyMap.contains("statsKey")
          && series.contains(keyMap("statsKey").asInstanceOf[String])
        )
          series.get(keyMap("statsKey").asInstanceOf[String])
        else series
      println(s"--- [FETCHED RESULT] ---\n${objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(toPrint)}")
    }

    def run(args: Args): Unit = {
      if (args.keyJson.isEmpty && args.keyJsonFile.isEmpty) {
        throw new Exception("At least one of keyJson and keyJsonFile should be specified!")
      }
      val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
      def readMap: String => Map[String, AnyRef] = { json =>
        objectMapper.readValue(json, classOf[java.util.Map[String, AnyRef]]).asScala.toMap
      }
      def readMapList: String => Seq[Map[String, AnyRef]] = { jsonList =>
        objectMapper
          .readValue(jsonList, classOf[java.util.List[java.util.Map[String, AnyRef]]])
          .asScala
          .map(_.asScala.toMap)
          .toSeq
      }
      val keyMapList =
        if (args.keyJson.isDefined) {
          Try(readMapList(args.keyJson())).toOption.getOrElse(Seq(readMap(args.keyJson())))
        } else {
          println(s"Reading requests from ${args.keyJsonFile()}")
          val file = Source.fromFile(args.keyJsonFile())
          val mapList = file.getLines().map(json => readMap(json)).toList
          file.close()
          mapList
        }
      if (keyMapList.length > 1) {
        println(s"Plan to send ${keyMapList.length} fetches with ${args.interval()} seconds interval")
      }
      val fetcher = args.impl(args.serializableProps).buildFetcher(true)
      def iterate(): Unit = {
        keyMapList.foreach(keyMap => {
          println(s"--- [START FETCHING for ${keyMap}] ---")
          if (args.`type`() == "join-stats") {
            fetchStats(args, objectMapper, keyMap, fetcher)
          } else {
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
                case Success(valMap) => {
                  if (valMap == null) {
                    println("No data present for the provided key.")
                  } else {
                    valMap.foreach { case (k, v) => tMap.put(k, v) }
                    println(
                      s"--- [FETCHED RESULT] ---\n${objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(tMap)}")
                  }
                  println(s"Fetched in: $awaitTimeMs ms")
                }
                case Failure(exception) => {
                  exception.printStackTrace()
                }
              })
            Thread.sleep(args.interval() * 1000)
          }
        })
      }
      iterate()
      while (args.loop()) {
        println("loop is set to true, start next iteration. will only exit if manually killed.")
        iterate()
      }
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

  object LogFlattener {
    class Args extends Subcommand("log-flattener") with OfflineSubcommand {
      val logTable: ScallopOption[String] =
        opt[String](required = true, descr = "Hive table with partitioned raw logs")

      val schemaTable: ScallopOption[String] =
        opt[String](required = true, descr = "Hive table with mapping from schema_hash to schema_value_last")

      val stepDays: ScallopOption[Int] =
        opt[Int](required = false,
                 descr = "Runs consistency metrics job in steps, step-days at a time. Default is 15 days",
                 default = Option(15))
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName() = s"log_flattener_join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val spark = args.sparkSession
      val logFlattenerJob = new LogFlattenerJob(
        spark,
        args.joinConf,
        args.endDate(),
        args.logTable(),
        args.schemaTable(),
        Some(args.stepDays())
      )
      logFlattenerJob.buildLogTable()
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
      val DEFAULT_LAG_MILLIS = 2000 // 2seconds
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
      val lagMillis: ScallopOption[Int] = opt[Int](
        required = false,
        default = Some(DEFAULT_LAG_MILLIS),
        descr = "Lag time for chaining, before fetching upstream join results, in milliseconds. Default 2 seconds"
      )
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
      implicit val session: SparkSession = SparkSessionBuilder.buildStreaming(args.debug())

      val confFile = findFile(args.confPath())
      val groupByConf = confFile
        .map(ThriftJsonCodec.fromJsonFile[api.GroupBy](_, check = false))
        .getOrElse(args.metaDataStore.getConf[api.GroupBy](args.confPath()).get)

      val onlineJar = findFile(args.onlineJar())
      if (args.debug())
        onlineJar.foreach(session.sparkContext.addJar)
      implicit val apiImpl = args.impl(args.serializableProps)
      val query = if (groupByConf.streamingSource.get.isSetJoinSource) {
        new JoinSourceRunner(groupByConf,
                             args.serializableProps,
                             args.debug(),
                             args.lagMillis.getOrElse(2000)).chainedStreamingQuery.start()
      } else {
        val streamingSource = groupByConf.streamingSource
        assert(streamingSource.isDefined,
               "There is no valid streaming source - with a valid topic, and endDate < today")
        lazy val host = streamingSource.get.topicTokens.get("host")
        lazy val port = streamingSource.get.topicTokens.get("port")
        if (!args.kafkaBootstrap.isDefined)
          assert(
            host.isDefined && port.isDefined,
            "Either specify a kafkaBootstrap url or provide host and port in your topic definition as topic/host=host/port=port")
        val inputStream: DataFrame =
          dataStream(session, args.kafkaBootstrap.getOrElse(s"${host.get}:${port.get}"), streamingSource.get.cleanTopic)
        new streaming.GroupBy(inputStream, session, groupByConf, args.impl(args.serializableProps), args.debug()).run()
      }
      query.awaitTermination()
    }
  }

  class Args(args: Array[String]) extends ScallopConf(args) {
    object JoinBackFillArgs extends JoinBackfill.Args
    addSubcommand(JoinBackFillArgs)
    object LogFlattenerArgs extends LogFlattener.Args
    addSubcommand(LogFlattenerArgs)
    object ConsistencyMetricsArgs extends ConsistencyMetricsCompute.Args
    addSubcommand(ConsistencyMetricsArgs)
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
    object AnalyzerArgs extends Analyzer.Args
    addSubcommand(AnalyzerArgs)
    object DailyStatsArgs extends DailyStats.Args
    addSubcommand(DailyStatsArgs)
    object CompareJoinQueryArgs extends CompareJoinQuery.Args
    addSubcommand(CompareJoinQueryArgs)
    object MetadataExportArgs extends MetadataExport.Args
    addSubcommand(MetadataExportArgs)
    object LabelJoinArgs extends LabelJoin.Args
    addSubcommand(LabelJoinArgs)
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

          case args.MetadataUploaderArgs   => MetadataUploader.run(args.MetadataUploaderArgs)
          case args.FetcherCliArgs         => FetcherCli.run(args.FetcherCliArgs)
          case args.LogFlattenerArgs       => LogFlattener.run(args.LogFlattenerArgs)
          case args.ConsistencyMetricsArgs => ConsistencyMetricsCompute.run(args.ConsistencyMetricsArgs)
          case args.CompareJoinQueryArgs   => CompareJoinQuery.run(args.CompareJoinQueryArgs)
          case args.AnalyzerArgs           => Analyzer.run(args.AnalyzerArgs)
          case args.DailyStatsArgs         => DailyStats.run(args.DailyStatsArgs)
          case args.MetadataExportArgs     => MetadataExport.run(args.MetadataExportArgs)
          case args.LabelJoinArgs          => LabelJoin.run(args.LabelJoinArgs)
          case _                           => println(s"Unknown subcommand: $x")
        }
      case None => println(s"specify a subcommand please")
    }
    if (shouldExit) {
      System.exit(0)
    }
  }
}
