package ai.chronon.spark.test

import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.{Fetcher, JoinCodec, LoggableResponseBase64, MetadataStore}
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.{Conversions, LogFlattenerJob, SparkSessionBuilder, TableUtils}
import junit.framework.TestCase
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, rank}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertFalse, assertNotEquals, assertTrue}

import java.nio.charset.StandardCharsets
import java.util.{Base64, TimeZone}
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.ScalaVersionSpecificCollectionsConverter

case class GroupByTestSuite(
    name: String,
    groupByConf: GroupBy,
    groupByData: DataFrame
)

case class JoinTestSuite(
    joinConf: Join,
    groupBys: Seq[GroupByTestSuite],
    fetchExpectations: (Map[String, AnyRef], Map[String, AnyRef])
)

object JoinTestSuite {

  def apply(joinConf: Join, groupBys: Seq[GroupByTestSuite]): JoinTestSuite = {
    val suite = JoinTestSuite(joinConf, groupBys)
    assert(
      groupBys.map(_.groupByConf.metaData.name) ==
        ScalaVersionSpecificCollectionsConverter
          .convertJavaListToScala(
            joinConf.joinParts
          )
          .map(_.groupBy.metaData.name)
    )
    suite
  }
}

class SchemaEvolutionTest extends TestCase {

  val spark: SparkSession = SparkSessionBuilder.build("SchemaEvolutionTest", local = true)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val fetchingDs = "2022-10-03"

  def createViewsGroupBy(namespace: String): GroupByTestSuite = {
    val name = "listing_views"
    val schema = StructType(
      name,
      Array(
        StructField("listing_id", LongType),
        StructField("m_guests", LongType),
        StructField("m_views", LongType),
        StructField("ts", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 2L, 20L, "2022-10-01 10:00:00", "2022-10-01"),
      Row(1L, 3L, 30L, "2022-10-02 10:00:00", "2022-10-02"),
      Row(2L, 1L, 10L, "2022-10-01 10:00:00", "2022-10-01"),
      Row(2L, 2L, 20L, "2022-10-02 10:00:00", "2022-10-02")
    )
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "m_guests" -> "m_guests",
          "m_views" -> "m_views"
        ),
        timeColumn = "UNIX_TIMESTAMP(ts) * 1000"
      ),
      table = s"${namespace}.${name}",
      topic = null,
      isCumulative = false
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "m_guests",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "m_views",
          windows = null
        )
      ),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"unit_test/${name}", namespace = namespace, team = "chronon")
    )
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    GroupByTestSuite(
      name,
      conf,
      df
    )
  }

  def createAttributesGroupBy(namespace: String): GroupByTestSuite = {
    val name = "listing_attributes"
    val schema = StructType(
      "listing_attributes",
      Array(
        StructField("listing_id", LongType),
        StructField("dim_bedrooms", IntType),
        StructField("dim_room_type", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 4, "ENTIRE_HOME", "2022-10-01"),
      Row(1L, 4, "ENTIRE_HOME", "2022-10-02"),
      Row(1L, 4, "ENTIRE_HOME", "2022-10-03"),
      Row(2L, 1, "PRIVATE_ROOM", "2022-10-01"),
      Row(2L, 1, "PRIVATE_ROOM", "2022-10-02"),
      Row(2L, 1, "PRIVATE_ROOM", "2022-10-03")
    )
    val source = Builders.Source.entities(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "dim_bedrooms" -> "dim_bedrooms",
          "dim_room_type" -> "dim_room_type"
        )
      ),
      snapshotTable = s"${namespace}.${name}"
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = null,
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"unit_test/${name}", namespace = namespace, team = "chronon")
    )
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    GroupByTestSuite(
      name,
      conf,
      df
    )
  }

  def createCheckoutsGroupBy(namespace: String): GroupByTestSuite = {
    val name = "checkouts"
    val schema = StructType(
      "checkouts",
      Array(
        StructField("user_id", LongType),
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 1L, 1664625600000L, "2022-10-01"), // "2022-10-01 12:00:00.000"
      Row(2L, 1L, 1664625600000L, "2022-10-01"), // "2022-10-01 12:00:00.000"
      Row(3L, 1L, 1664625600000L, "2022-10-01"), // "2022-10-01 12:00:00.000"
      Row(2L, 2L, 1664712000000L, "2022-10-02"), // "2022-10-02 12:00:00.000"
      Row(3L, 2L, 1664712000000L, "2022-10-02"), // "2022-10-02 12:00:00.000"
      Row(3L, 3L, 1664740800000L, "2022-10-02") // "2022-10-02 20:00:00.000"
    )
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Map(
          "user" -> "user_id",
          "listing" -> "listing_id"
        ),
        timeColumn = "ts"
      ),
      table = s"${namespace}.${name}"
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "listing",
          windows = null
        )
      ),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"unit_test/${name}", namespace = namespace, team = "chronon")
    )
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    GroupByTestSuite(
      name,
      conf,
      df
    )
  }

  def createTestDriverTable(namespace: String, selects: Seq[String]): Source = {
    Builders.Source.events(
      Builders.Query(
        selects = Builders.Selects(selects: _*),
        timeColumn = "ts"
      ),
      table = s"$namespace.test_driver_table"
    )
  }

  def createV1Join(namespace: String): JoinTestSuite = {
    val viewsGroupBy = createViewsGroupBy(namespace)
    val joinConf = Builders.Join(
      left = createTestDriverTable(namespace, Seq("listing")),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy.groupByConf)),
      metaData =
        Builders.MetaData(name = "unit_test/test_join", namespace = namespace, team = "chronon", samplePercent = 1.0)
    )

    JoinTestSuite(
      joinConf,
      Seq(viewsGroupBy),
      (
        Map("listing" -> 1L.asInstanceOf[AnyRef]),
        Map(
          "unit_test_listing_views_m_guests_sum" -> 5L.asInstanceOf[AnyRef],
          "unit_test_listing_views_m_views_sum" -> 50L.asInstanceOf[AnyRef]
        )
      )
    )
  }

  def createV2Join(namespace: String): JoinTestSuite = {
    val viewsGroupBy = createViewsGroupBy(namespace)
    val attributesGroupBy = createAttributesGroupBy(namespace)
    val joinConf = Builders.Join(
      left = createTestDriverTable(namespace, Seq("listing")),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy.groupByConf),
        Builders.JoinPart(groupBy = attributesGroupBy.groupByConf)
      ),
      metaData =
        Builders.MetaData(name = "unit_test/test_join", namespace = namespace, team = "chronon", samplePercent = 1.0)
    )
    JoinTestSuite(
      joinConf,
      Seq(viewsGroupBy, attributesGroupBy),
      (
        Map("listing" -> 1L.asInstanceOf[AnyRef]),
        Map(
          "unit_test_listing_views_m_guests_sum" -> 5L.asInstanceOf[AnyRef],
          "unit_test_listing_views_m_views_sum" -> 50L.asInstanceOf[AnyRef],
          "unit_test_listing_attributes_dim_bedrooms" -> 4.asInstanceOf[AnyRef],
          "unit_test_listing_attributes_dim_room_type" -> "ENTIRE_HOME"
        )
      )
    )
  }

  def createV3Join(namespace: String): JoinTestSuite = {
    val viewsGroupBy = createViewsGroupBy(namespace)
    val checkoutsGroupBy = createCheckoutsGroupBy(namespace)
    val joinConf = Builders.Join(
      left = createTestDriverTable(namespace, Seq("listing", "user")),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy.groupByConf),
        Builders.JoinPart(groupBy = checkoutsGroupBy.groupByConf)
      ),
      metaData =
        Builders.MetaData(name = "unit_test/test_join", namespace = namespace, team = "chronon", samplePercent = 1.0)
    )
    JoinTestSuite(
      joinConf,
      Seq(viewsGroupBy, checkoutsGroupBy),
      (
        Map(
          "listing" -> 1L.asInstanceOf[AnyRef],
          "user" -> 3L.asInstanceOf[AnyRef]
        ),
        Map(
          "unit_test_listing_views_m_guests_sum" -> 5L.asInstanceOf[AnyRef],
          "unit_test_listing_views_m_views_sum" -> 50L.asInstanceOf[AnyRef],
          "unit_test_checkouts_listing_count" -> 3L.asInstanceOf[AnyRef]
        )
      )
    )
  }

  private def fetchJoin(fetcher: Fetcher, joinTestSuite: JoinTestSuite): Fetcher.Response = {
    val request = Request(joinTestSuite.joinConf.metaData.nameToFilePath, joinTestSuite.fetchExpectations._1)
    val future = fetcher.fetchJoin(Seq(request))
    val responses = Await.result(future, Duration(10000, SECONDS)).toSeq
    assertEquals(1, responses.length)
    responses.head
  }

  private def runGBUpload(
      namespace: String,
      joinTestSuite: JoinTestSuite,
      tableUtils: TableUtils,
      inMemoryKvStore: InMemoryKvStore
  ): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    joinTestSuite.groupBys.foreach { gbTestSuite =>
      val tableName = s"${namespace}.${gbTestSuite.name}"
      gbTestSuite.groupByData.save(tableName)
      OnlineUtils.serve(
        tableUtils,
        inMemoryKvStore,
        () => inMemoryKvStore,
        namespace,
        fetchingDs,
        gbTestSuite.groupByConf
      )
    }
  }

  private def clearTTLCache(fetcher: Fetcher): Unit = {
    fetcher.getJoinCodecs.cMap.clear()
    fetcher.getJoinConf.cMap.clear()
    fetcher.getGroupByServingInfo.cMap.clear()
  }

  private def extractDataEventAndControlEvent(
      logs: Seq[LoggableResponseBase64]): (LoggableResponseBase64, LoggableResponseBase64) = {
    assertEquals(2, logs.length)
    val controlEvent = logs.filter(_.name == Constants.SchemaPublishEvent).head
    val dataEvent = logs.filter(_.name != Constants.SchemaPublishEvent).head
    assertEquals(
      Base64.getEncoder.encodeToString(dataEvent.schemaHash.getBytes(Constants.UTF8)),
      controlEvent.keyBase64
    )
    (dataEvent, controlEvent)
  }

  private def insertLogsToHive(mockApi: MockApi, logs: Seq[LoggableResponseBase64], ds: String): Unit = {
    val logDf = mockApi.loggedValuesToDf(logs, spark)

    // populate timestamp for each row that is unique and falls within ds
    val adjustedDf = logDf
      .withColumn("ds", lit(ds))
      .withColumn("id", rank().over(Window.orderBy("ts_millis")))
      .withPartitionBasedTimestamp("ts_millis")
      .withColumn("ts_millis", col("ts_millis") + col("id"))
      .drop("id")
    TableUtils(spark).insertPartitions(
      adjustedDf,
      mockApi.logTable,
      partitionColumns = Seq("ds", "name")
    )
  }

  /*
   * Run through (1) dump raw logs into hive, (2) construct schema table, (3) run log flattening
   */
  private def verifyOfflineTables(logsDelta: Seq[LoggableResponseBase64],
                                  offlineDs: String,
                                  mockApi: MockApi,
                                  joinConf: Join,
                                  tableUtils: TableUtils): DataFrame = {
    insertLogsToHive(mockApi, logsDelta, offlineDs)
    SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, offlineDs, "2022-10-01")
    val flattenerJob = new LogFlattenerJob(spark, joinConf, offlineDs, mockApi.logTable, mockApi.schemaTable)
    flattenerJob.buildLogTable()
    val flattenedDf = spark
      .table(joinConf.metaData.loggedTable)
      .where(col(Constants.PartitionColumn) === offlineDs)
    assertEquals(2, flattenedDf.count())
    assertTrue(
      LogFlattenerJob
        .readSchemaTableProperties(tableUtils, joinConf)
        .mapValues(JoinCodec.fromLoggingSchema(_, joinConf))
        .values
        .nonEmpty)
    flattenedDf
  }

  /*
   * Simulate a stagingQuery that saves listing/ts pairs from flattened log table into a Hive table to be
   * served as the left of the join.
   *
   * newKeys specifies new feature keys added in this join, which we will manually add to the DF
   */
  private def prepareDriverTable(ds: String,
                                 joinConf: Join,
                                 namespace: String,
                                 tableUtils: TableUtils,
                                 keysFallback: Map[String, String] = Map.empty): DataFrame = {

    val keys =
      ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(joinConf.left.getEvents.query.selects).keys
    // handle new keys during schema evolution: fill as NULL
    val selects = keys.map { key =>
      if (keysFallback.contains(key)) {
        key -> keysFallback(key)
      } else {
        key -> key
      }
    }.toMap
    val notNullFilter = s"(${keys.filterNot(keysFallback.contains).map(k => s"$k IS NOT NULL").mkString(" OR ")})"
    val query = QueryUtils.build(
      selects ++ Map("ts" -> "ts", "ds" -> "ds"),
      from = joinConf.metaData.loggedTable,
      wheres = Seq(notNullFilter, s"${Constants.PartitionColumn} = '$ds'", "ts IS NOT NULL")
    )
    val df = tableUtils.sql(query)
    tableUtils.insertPartitions(df, s"$namespace.test_driver_table", autoExpand = true)
    df
  }

  private def runJoinCompute(ds: String, joinConf: Join, tableUtils: TableUtils): DataFrame = {

    val joinJob = new ai.chronon.spark.Join(
      joinConf,
      ds,
      tableUtils
    )
    joinJob.computeJoin()
    val df = spark.table(joinConf.metaData.outputTable)
    df
  }

  def testSchemaEvolution(namespace: String, joinSuiteV1: JoinTestSuite, joinSuiteV2: JoinTestSuite): Unit = {
    assert(joinSuiteV1.joinConf.metaData.name == joinSuiteV2.joinConf.metaData.name,
           message = "Schema evolution can only be tested on changes of the SAME join")
    val tableUtils: TableUtils = TableUtils(spark)
    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore(namespace)
    val mockApi = new MockApi(() => inMemoryKvStore, namespace)
    inMemoryKvStore.create(ChrononMetadataKey)
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)

    /* STAGE 1: Create join v1 and upload the conf to MetadataStore */
    metadataStore.putJoinConf(joinSuiteV1.joinConf)
    val fetcher = mockApi.buildFetcher(true)
    val response1 = fetchJoin(fetcher, joinSuiteV1)
    assertTrue(response1.values.get.keys.exists(_.endsWith("_exception")))
    assertEquals(joinSuiteV1.groupBys.length, response1.values.get.keys.size)

    // empty responses are still logged and this schema version is still tracked
    val logs1 = mockApi.flushLoggedValues
    val (dataEvent1, _) = extractDataEventAndControlEvent(logs1)
    assertEquals("", dataEvent1.keyBase64)
    assertEquals("", dataEvent1.valueBase64)

    /* STAGE 2: GroupBy upload completes and start having successful fetches & logs */
    runGBUpload(namespace, joinSuiteV1, tableUtils, inMemoryKvStore)
    clearTTLCache(fetcher)
    val response2 = fetchJoin(fetcher, joinSuiteV1)
    assertEquals(joinSuiteV1.fetchExpectations._2, response2.values.get)

    val logs2 = mockApi.flushLoggedValues
    val (dataEvent2, controlEvent2) = extractDataEventAndControlEvent(logs2)
    val schema2 = new String(Base64.getDecoder.decode(controlEvent2.valueBase64), StandardCharsets.UTF_8)
    val joinV1Codec = JoinCodec.fromLoggingSchema(schema2, joinSuiteV1.joinConf)
    assertEquals(dataEvent2.schemaHash, joinV1Codec.loggingSchemaHash)

    val offlineDs12 = "2022-10-03"
    val flattenedDf12 = verifyOfflineTables(
      logs1 ++ logs2, // combine logs from stage 1 and stage 2 into offline DS = 2022-10-03
      offlineDs = offlineDs12,
      mockApi,
      joinSuiteV1.joinConf,
      tableUtils
    )

    // run driver table preparation to insert from logs of ds1
    val driverDf2 = prepareDriverTable(offlineDs12, joinSuiteV1.joinConf, namespace, tableUtils)
    assertEquals(1, driverDf2.count())

    // run join to pull from logs
    val joinDf2 = runJoinCompute(offlineDs12, joinSuiteV1.joinConf, tableUtils)
    assertEquals(1, joinDf2.count())

    /* STAGE 3: Join is modified and updated to MetadataStore */
    metadataStore.putJoinConf(joinSuiteV2.joinConf)
    clearTTLCache(fetcher)
    val response3 = fetchJoin(fetcher, joinSuiteV2)

    val newGroupBys = joinSuiteV2.groupBys.filter(gb => !joinSuiteV1.groupBys.exists(g => g.name == gb.name))
    val existingGroupBys = joinSuiteV2.groupBys.filter(gb => joinSuiteV1.groupBys.exists(g => g.name == gb.name))
    val removedGroupBys = joinSuiteV1.groupBys.filter(gb => !joinSuiteV2.groupBys.exists(g => g.name == gb.name))
    val existingSubMapExpected = joinSuiteV2.fetchExpectations._2.filter {
      case (key, _) => existingGroupBys.exists(gb => key.contains(gb.name))
    }
    val newSubMapExpected = joinSuiteV2.fetchExpectations._2.filter {
      case (key, _) => newGroupBys.exists(gb => key.contains(gb.name))
    }
    val newSubMapActual = response3.values.get.filter {
      case (key, _) => newGroupBys.exists(gb => key.contains(gb.name))
    }
    val existingSubMapActual = response3.values.get.filter {
      case (key, _) => existingGroupBys.exists(gb => key.contains(gb.name))
    }
    val removedSubMapOriginalData = joinSuiteV1.fetchExpectations._2.filter {
      case (key, _) => removedGroupBys.exists(gb => key.contains(gb.name))
    }
    val newFeatures = newSubMapExpected.keySet
    val removedFeatures = removedSubMapOriginalData.keySet

    assertEquals(existingSubMapActual, existingSubMapExpected)
    val newGroupByCount = newGroupBys.length
    assertEquals(newGroupByCount, newSubMapActual.keys.size)
    if (newGroupByCount > 0) {
      // new GroupBy fetches will fail because upload has not run
      assertTrue(newSubMapActual.keys.exists(_.endsWith("_exception")))
    }
    assertFalse(response3.values.get.keys.exists(k => removedSubMapOriginalData.keys.toSet.contains(k)))

    val logs3 = mockApi.flushLoggedValues
    val (dataEvent3, _) = extractDataEventAndControlEvent(logs3)
    if (removedGroupBys.isEmpty) {
      // verify that schemaHash is NOT changed in this scenario because newly added GroupBys are skipped
      // because GroupByUpload for them has NOT run and GBServingInfo is not found.
      assertEquals(dataEvent2.schemaHash, dataEvent3.schemaHash)
    } else {
      // verify that schemaHash is changed because some groupBys are removed from the join
      assertNotEquals(dataEvent2.schemaHash, dataEvent3.schemaHash)
    }

    // run GB upload now to prepare the offline tables and then verify the offline backfill behavior
    runGBUpload(namespace, joinSuiteV2, tableUtils, inMemoryKvStore)
    val offlineDs34 = "2022-10-04" // override ds to simplify offline data generation
    val newKeys = ScalaVersionSpecificCollectionsConverter
      .convertJavaMapToScala(joinSuiteV2.joinConf.left.getEvents.query.selects)
      .keys
      .filterNot(joinSuiteV1.joinConf.left.getEvents.query.selects.keySet().contains)

    // rerun driver preparation for ds1 to add new column
    val driverDf3 =
      prepareDriverTable(offlineDs12, joinSuiteV2.joinConf, namespace, tableUtils, newKeys.map(k => k -> "3L").toMap)
    assertEquals(1, driverDf3.count())

    // run join to backfill new features
    val joinDf3 = runJoinCompute(offlineDs12, joinSuiteV2.joinConf, tableUtils)
    assertEquals(1, joinDf3.count())
    assertTrue(newFeatures.forall(joinDf3.columns.contains))

    /* STAGE 4: GroupBy upload completes for the new GroupBy */
    clearTTLCache(fetcher)
    val response4 = fetchJoin(fetcher, joinSuiteV2)
    assertEquals(joinSuiteV2.fetchExpectations._2, response4.values.get)

    val logs4 = mockApi.flushLoggedValues
    val (dataEvent4, controlEvent4) = extractDataEventAndControlEvent(logs4)
    if (newGroupBys.nonEmpty) {
      // verify that schemaHash is changed in this scenario because newly added GroupBys are now being successfully
      // fetched.
      assertNotEquals(dataEvent3.schemaHash, dataEvent4.schemaHash)
    } else {
      // verify that schemaHash is unchanged if there is no new groupBys added
      assertEquals(dataEvent3.schemaHash, dataEvent4.schemaHash)
    }
    val schema4 = new String(Base64.getDecoder.decode(controlEvent4.valueBase64), StandardCharsets.UTF_8)
    val joinV2Codec = JoinCodec.fromLoggingSchema(schema4, joinSuiteV2.joinConf)
    assertEquals(dataEvent4.schemaHash, joinV2Codec.loggingSchemaHash)

    val flattenedDf34 = verifyOfflineTables(
      logs3 ++ logs4, // combine logs from stage 3 and stage 4 into offline DS = 2022-10-04
      offlineDs = offlineDs34,
      mockApi,
      joinSuiteV2.joinConf,
      tableUtils
    )

    /* new features are appended as new columns */
    assertTrue(newFeatures.forall(!flattenedDf12.schema.fieldNames.contains(_)))
    assertTrue(newFeatures.forall(flattenedDf34.schema.fieldNames.contains(_)))

    /* removed features are never removed from the table */
    assertTrue(removedFeatures.forall(flattenedDf12.schema.fieldNames.contains(_)))
    assertTrue(removedFeatures.forall(flattenedDf34.schema.fieldNames.contains(_)))

    // run driver preparation for ds2 to insert the logs of ds2
    val driverDf4 = prepareDriverTable(offlineDs34,
                                       joinSuiteV2.joinConf,
                                       namespace,
                                       tableUtils,
                                       newKeys.map(k => k -> s"COALESCE($k, 3L)").toMap)
    assertEquals(2, driverDf4.count())

    // run join for new ds and expect to pull from logs
    val joinDf4 = runJoinCompute(offlineDs34, joinSuiteV2.joinConf, tableUtils)
    assertEquals(3, joinDf4.count())
    assertTrue(newFeatures.forall(feat => joinDf4.collect().forall(row => !row.isNullAt(row.fieldIndex(feat)))))
  }

  def testAddFeatures(): Unit = {
    val namespace = "add_features"
    testSchemaEvolution(namespace, createV1Join(namespace), createV2Join(namespace))
  }

  def testRemoveFeatures(): Unit = {
    val namespace = "remove_features"
    testSchemaEvolution(namespace, createV2Join(namespace), createV1Join(namespace))
  }

  def testAddKeyAndFeatures(): Unit = {
    val namespace = "add_key_and_features"
    testSchemaEvolution(namespace, createV1Join(namespace), createV3Join(namespace))
  }

  def testRemoveKeyAndFeatures(): Unit = {
    val namespace = "remove_key_and_features"
    testSchemaEvolution(namespace, createV3Join(namespace), createV1Join(namespace))
  }
}
