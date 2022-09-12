package ai.chronon.spark.test

import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.{Fetcher, JoinCodec, MetadataStore, TTLCache}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Conversions, SparkSessionBuilder, TableUtils}
import junit.framework.TestCase
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.{assertEquals, assertFalse, assertNotEquals, assertTrue}

import java.nio.charset.StandardCharsets
import java.util.{Base64, TimeZone}
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}

class SchemaEvolutionTest extends TestCase {

  val sessionName = "SchemaEvolutionTest"
  val spark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val fetchingDs = "2022-10-03"

  private lazy val viewsSourceSchema = StructType("listing_views",
    Array(
      StructField("listing_id", LongType),
      StructField("m_guests", LongType),
      StructField("m_views", LongType),
      StructField("ts", StringType),
      StructField("ds", StringType)
    ))

  private lazy val viewsSourceData = Seq(
    Row(1L, 2L, 20L, "2022-10-01 10:00:00", "2022-10-01"),
    Row(1L, 3L, 30L, "2022-10-02 10:00:00", "2022-10-02"),
    Row(2L, 1L, 10L, "2022-10-01 10:00:00", "2022-10-01"),
    Row(2L, 2L, 20L, "2022-10-02 10:00:00", "2022-10-02")
  )

  def viewsSource(namespace: String): Source = Builders.Source.events(
    query = Builders.Query(
      selects = Map(
        "listing" -> "listing_id",
        "m_guests" -> "m_guests",
        "m_views" -> "m_views"
      ),
      timeColumn = "UNIX_TIMESTAMP(ts) * 1000"
    ),
    table = s"${namespace}.${viewsSourceSchema.name}",
    topic = null,
    isCumulative = false
  )

  def viewsGB(namespace: String): GroupBy = Builders.GroupBy(
    sources = Seq(viewsSource(namespace)),
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
    metaData = Builders.MetaData(name = "unit_test/listing_views_v1", namespace = namespace, team = "chronon")
  )

  private lazy val attributesSourceSchema = StructType("listing_attributes",
    Array(
      StructField("listing_id", LongType),
      StructField("dim_bedrooms", IntType),
      StructField("dim_room_type", StringType),
      StructField("ds", StringType)
    ))

  private lazy val attributesSourceData = Seq(
    Row(1L, 4, "ENTIRE_HOME", "2022-10-01"),
    Row(1L, 4, "ENTIRE_HOME", "2022-10-02"),
    Row(2L, 1, "PRIVATE_ROOM", "2022-10-01"),
    Row(2L, 1, "PRIVATE_ROOM", "2022-10-02")
  )

  def attributesSource(namespace: String) = Builders.Source.entities(
    query = Builders.Query(
      selects = Map(
        "listing" -> "listing_id",
        "dim_bedrooms" -> "dim_bedrooms",
        "dim_room_type" -> "dim_room_type"
      )
    ),
    snapshotTable = s"${namespace}.${attributesSourceSchema.name}"
  )

  def attributesGB(namespace: String): GroupBy = Builders.GroupBy(
    sources = Seq(attributesSource(namespace)),
    keyColumns = Seq("listing"),
    aggregations = null,
    accuracy = Accuracy.SNAPSHOT,
    metaData = Builders.MetaData(name = "unit_test/listing_attributes_v1", namespace = namespace, team = "chronon")
  )

  private lazy val joinName = "unit_test/test_join"
  def joinV1(namespace: String): Join = Builders.Join(
    left = viewsSource(namespace),
    joinParts = Seq(Builders.JoinPart(groupBy = viewsGB(namespace))),
    metaData = Builders.MetaData(name = joinName, namespace = namespace, team = "chronon")
  )

  def joinV2(namespace: String): Join = Builders.Join(
    left = viewsSource(namespace),
    joinParts = Seq(
      Builders.JoinPart(groupBy = viewsGB(namespace)),
      Builders.JoinPart(groupBy = attributesGB(namespace))
    ),
    metaData = Builders.MetaData(name = joinName, namespace = namespace, team = "chronon")
  )

  private def runGBUpload(
                           namespace: String,
                           groupByConf: GroupBy,
                           sourceSchema: StructType,
                           sourceData: Seq[Row],
                           tableUtils: TableUtils,
                           inMemoryKvStore: InMemoryKvStore
                         ): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val tableName = s"$namespace.${sourceSchema.name}"
    spark.createDataFrame(sourceData.asJava, Conversions.fromChrononSchema(sourceSchema)).save(tableName)
    OnlineUtils.serve(
      tableUtils,
      inMemoryKvStore,
      () => inMemoryKvStore,
      namespace,
      fetchingDs,
      groupByConf
    )
  }

  private def fetchJoin(namespace: String, fetcher: Fetcher): Fetcher.Response = {
    // join name is guaranteed to be the same since we iterate on the same join over time
    val name = joinV1(namespace).metaData.nameToFilePath
    val request = Request(name, Map("listing" -> 1L.asInstanceOf[AnyRef]))
    val future = fetcher.fetchJoin(Seq(request))
    val responses = Await.result(future, Duration(10000, SECONDS)).toSeq
    assertEquals(1, responses.length)
    responses.head
  }

  private def clearTTLCache(fetcher: Fetcher): Unit = {
    fetcher.getJoinCodecs.cMap.clear()
    fetcher.getJoinConf.cMap.clear()
    fetcher.getGroupByServingInfo.cMap.clear()
  }

  def testSchemaEvolution(): Unit = {
    val namespace = "test_schema_evolution"
    val tableUtils: TableUtils = TableUtils(spark)
    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore(sessionName)
    val mockApi = new MockApi(() => inMemoryKvStore, namespace)
    inMemoryKvStore.create(ChrononMetadataKey)
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)

    /* STAGE 1: Create join v1 and upload the conf to MetadataStore */
    metadataStore.putJoinConf(joinV1(namespace))
    val fetcher = mockApi.buildFetcher(true)
    var response = fetchJoin(namespace, fetcher)

    assertTrue(response.values.get.contains("unit_test/listing_views_v1_exception"))
    assertEquals(1, response.values.get.keys.size)

    // empty responses are still logged and this schema version is still tracked
    var logs = mockApi.flushLoggedValues
    assertEquals(2, logs.length)
    var controlEvent = logs.filter(_.name == Constants.SchemaPublishEvent).head
    var dataEvent = logs.filter(_.name != Constants.SchemaPublishEvent).head
    assertEquals(
      Base64.getEncoder.encodeToString(dataEvent.schemaHash.getBytes(Constants.UTF8)),
      controlEvent.keyBase64
    )
    assertEquals("", dataEvent.keyBase64)
    assertEquals("", dataEvent.valueBase64)

    /* STAGE 2: GroupBy upload completes and start having successful fetches & logs */
    runGBUpload(namespace, viewsGB(namespace), viewsSourceSchema, viewsSourceData, tableUtils, inMemoryKvStore)
    clearTTLCache(fetcher)
    response = fetchJoin(namespace, fetcher)

    assertEquals(
      Map(
        "unit_test_listing_views_v1_m_guests_sum" -> 5L,
        "unit_test_listing_views_v1_m_views_sum" -> 50L
      ),
      response.values.get
    )

    logs = mockApi.flushLoggedValues
    assertEquals(2, logs.length)
    controlEvent = logs.filter(_.name == Constants.SchemaPublishEvent).head
    dataEvent = logs.filter(_.name != Constants.SchemaPublishEvent).head
    assertEquals(
      Base64.getEncoder.encodeToString(dataEvent.schemaHash.getBytes(Constants.UTF8)),
      controlEvent.keyBase64
    )
    var schemaHash = dataEvent.schemaHash
    var schemaValue = new String(Base64.getDecoder.decode(controlEvent.valueBase64), StandardCharsets.UTF_8)
    val joinV1Codec = JoinCodec.fromLoggingSchema(schemaValue, joinV1(namespace))
    assertEquals(schemaHash, joinV1Codec.loggingSchemaHash)

    /* STAGE 3: Join is modified and updated to MetadataStore */
    metadataStore.putJoinConf(joinV2(namespace))
    clearTTLCache(fetcher)
    response = fetchJoin(namespace, fetcher)

    assertEquals(5L, response.values.get("unit_test_listing_views_v1_m_guests_sum"))
    assertEquals(50L, response.values.get("unit_test_listing_views_v1_m_views_sum"))
    assertTrue(response.values.get.contains("unit_test/listing_attributes_v1_exception"))

    logs = mockApi.flushLoggedValues
    assertEquals(2, logs.length)
    controlEvent = logs.filter(_.name == Constants.SchemaPublishEvent).head
    dataEvent = logs.filter(_.name != Constants.SchemaPublishEvent).head

    // verify that schemaHash is NOT changed in this scenario because we skip failed JoinPart
    assertEquals(schemaHash, dataEvent.schemaHash)
    assertEquals(schemaHash, new String(Base64.getDecoder.decode(controlEvent.keyBase64), StandardCharsets.UTF_8))

    /* STAGE 4: GroupBy upload completes for the new GroupBy */
    runGBUpload(namespace, attributesGB(namespace), attributesSourceSchema, attributesSourceData, tableUtils, inMemoryKvStore)
    clearTTLCache(fetcher)
    response = fetchJoin(namespace, fetcher)

    assertEquals(4, response.values.get.keys.size)
    assertEquals(4, response.values.get("unit_test_listing_attributes_v1_dim_bedrooms"))
    assertEquals("ENTIRE_HOME", response.values.get("unit_test_listing_attributes_v1_dim_room_type"))

    logs = mockApi.flushLoggedValues
    assertEquals(2, logs.length)
    controlEvent = logs.filter(_.name == Constants.SchemaPublishEvent).head
    dataEvent = logs.filter(_.name != Constants.SchemaPublishEvent).head

    assertNotEquals(schemaHash, dataEvent.schemaHash)
    schemaHash = new String(Base64.getDecoder.decode(controlEvent.keyBase64), StandardCharsets.UTF_8)
    schemaValue = new String(Base64.getDecoder.decode(controlEvent.valueBase64), StandardCharsets.UTF_8)
    assertEquals(schemaHash, dataEvent.schemaHash)
    val joinV2Codec = JoinCodec.fromLoggingSchema(schemaValue, joinV2(namespace))
    assertEquals(schemaHash, joinV2Codec.loggingSchemaHash)
    assertEquals(4, joinV2Codec.valueFields.length)
  }

  def testCacheInvalidate(): Unit = {
    val namespace = "test_cache_invalidate"
    val tableUtils: TableUtils = TableUtils(spark)
    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore(sessionName)
    val mockApi = new MockApi(() => inMemoryKvStore, namespace)
    inMemoryKvStore.create(ChrononMetadataKey)
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    val fetcher = mockApi.buildFetcher(true)

    runGBUpload(namespace, viewsGB(namespace), viewsSourceSchema, viewsSourceData, tableUtils, inMemoryKvStore)
    metadataStore.putJoinConf(joinV1(namespace))
    var response = fetchJoin(namespace, fetcher)
    var logs = mockApi.flushLoggedValues
    assertEquals(2, logs.length)
    val controlEvent = logs.filter(_.name == Constants.SchemaPublishEvent).head
    val schemaHash = new String(Base64.getDecoder.decode(controlEvent.keyBase64), StandardCharsets.UTF_8)

    Thread.sleep(TTLCache.DEFAULT_REFRESH_TTL_MILLIS)
    runGBUpload(namespace, attributesGB(namespace), attributesSourceSchema, attributesSourceData, tableUtils, inMemoryKvStore)
    metadataStore.putJoinConf(joinV2(namespace))
    fetcher.getJoinConf.cMap.clear()
    response = fetchJoin(namespace, fetcher)
    logs = mockApi.flushLoggedValues

    // joinCodec is stale so only old values are logged for 8 seconds
    assertEquals(1, logs.length)
    assertEquals(schemaHash, logs.head.schemaHash)

    response = fetchJoin(namespace, fetcher)
    logs = mockApi.flushLoggedValues
    assertEquals(2, logs.length)
    assertNotEquals(schemaHash, logs.head.schemaHash)
  }
}
