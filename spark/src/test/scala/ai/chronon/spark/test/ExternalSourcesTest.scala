/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.spark.LoggingSchema
import org.junit.Assert._
import org.junit.Test

import java.util.Base64
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.ScalaJavaConversions.JMapOps

class ExternalSourcesTest {
  @Test
  def testFetch(): Unit = {
    val plusOneSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "plus_one"
      ),
      keySchema = StructType("keys_plus_one", Array(StructField("number", IntType))),
      valueSchema = StructType("values_plus_one", Array(StructField("number", IntType)))
    )

    val alwaysFailsSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "always_fails"
      ),
      keySchema = StructType("keys_always_fails", Array(StructField("str", StringType))),
      valueSchema = StructType("values_always_fails", Array(StructField("str", StringType)))
    )

    val javaPlusOneSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "java_plus_one"
      ),
      keySchema =
        StructType("keys_java_plus_one", Array(StructField("number", IntType), StructField("number_mapped", IntType))),
      valueSchema =
        StructType("values_java_plus_one", Array(StructField("number", IntType), StructField("number_mapped", IntType)))
    )

    val contextualSource = Builders.ContextualSource(
      fields = Array(StructField("context_1", IntType), StructField("context_2", IntType))
    )

    val namespace = "external_source_test"
    val join = Builders.Join(
      // left defined here shouldn't really matter for this test
      left = Builders.Source.events(Builders.Query(selects = Map("number" -> "number", "str" -> "test_str")),
                                    table = "non_existent_table"),
      externalParts = Seq(
        Builders.ExternalPart(
          plusOneSource,
          prefix = "p1"
        ),
        Builders.ExternalPart(
          plusOneSource,
          prefix = "p2"
        ),
        Builders.ExternalPart(
          alwaysFailsSource
        ),
        Builders.ExternalPart(
          javaPlusOneSource,
          keyMapping = Map("number" -> "number_mapped"),
          prefix = "p3"
        ),
        Builders.ExternalPart(
          contextualSource
        )
      ),
      metaData = Builders.MetaData(name = "test/payments_join", namespace = namespace, team = "chronon")
    )

    // put this join into kv store
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("ExternalSourcesTest_testFetch")
    val mockApi = new MockApi(kvStoreFunc, "external_test")
    val fetcher = mockApi.buildFetcher(true)
    fetcher.kvStore.create(ChrononMetadataKey)
    fetcher.putJoinConf(join)

    val requests = (10 until 21).map(x =>
      Request(join.metaData.name,
              Map(
                "number" -> new Integer(x),
                "str" -> "a",
                "context_1" -> new Integer(2 + x),
                "context_2" -> new Integer(3 + x)
              )))
    val responsesF = fetcher.fetchJoin(requests)
    val responses = Await.result(responsesF, Duration(10, SECONDS))
    val numbers = mutable.HashSet.empty[Int]
    val keys = Set(
      "ext_p1_plus_one_number",
      "ext_p2_plus_one_number",
      "ext_always_fails_exception",
      "ext_p3_java_plus_one_number",
      "ext_p3_java_plus_one_number_mapped",
      "ext_contextual_context_1",
      "ext_contextual_context_2"
    )
    responses.map(_.values).foreach { m =>
      assertTrue(m.isSuccess)
      assertEquals(m.get.keysIterator.toSet, keys)
      numbers.add(m.get("ext_p1_plus_one_number").asInstanceOf[Int])
    }
    assert(numbers == (11 until 22).toSet)
    val logs = mockApi.flushLoggedValues
    val controlEvent = logs.find(_.name == Constants.SchemaPublishEvent).get
    val schema =
      LoggingSchema.parseLoggingSchema(new String(Base64.getDecoder.decode(controlEvent.valueBase64), Constants.UTF8))
    assertEquals(
      Set(
        "number",
        "str",
        "context_1",
        "context_2"
      ),
      schema.keyFields.fields.map(_.name).toSet
    )
    assertEquals(
      Set(
        "ext_p1_plus_one_number",
        "ext_p2_plus_one_number",
        "ext_always_fails_str",
        "ext_p3_java_plus_one_number",
        "ext_p3_java_plus_one_number_mapped",
        "ext_contextual_context_1",
        "ext_contextual_context_2"
      ),
      schema.valueFields.fields.map(_.name).toSet
    )
    assertEquals(responses.length + 1, logs.length)

    // test soft-fail on missing keys
    val emptyResponseF = fetcher.fetchJoin(Seq(Request(join.metaData.name, Map.empty)))
    val emptyResponseMap = Await.result(emptyResponseF, Duration(10, SECONDS)).head.values.get

    val expectedKeys = Set(
      "ext_p1_plus_one_exception",
      "ext_p2_plus_one_exception",
      "ext_p3_java_plus_one_exception",
      "ext_always_fails_exception",
      "ext_contextual_context_1",
      "ext_contextual_context_2"
    )
    assertEquals(expectedKeys, emptyResponseMap.keySet)
    assertEquals(null, emptyResponseMap("ext_contextual_context_1"))
    assertEquals(null, emptyResponseMap("ext_contextual_context_2"))
  }

  @Test
  def testFactoryBasedExternalSources(): Unit = {
    // Create factory configuration
    val factoryConfig = new ExternalSourceFactoryConfig()
    factoryConfig.setFactoryName("test-factory")
    factoryConfig.setFactoryParams(Map("increment" -> "2").toJava)

    // Create external source with factory configuration
    val factoryBasedSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "factory_plus_two"
      ),
      keySchema = StructType("keys_factory_plus_two", Array(StructField("number", IntType))),
      valueSchema = StructType("values_factory_plus_two", Array(StructField("number", IntType)))
    )
    factoryBasedSource.setFactoryConfig(factoryConfig)

    val namespace = "factory_test"
    val join = Builders.Join(
      left = Builders.Source.events(Builders.Query(selects = Map("number" -> "number")), table = "non_existent_table"),
      externalParts = Seq(
        Builders.ExternalPart(
          factoryBasedSource,
          prefix = "factory"
        )
      ),
      metaData = Builders.MetaData(name = "test/factory_join", namespace = namespace, team = "chronon")
    )

    // Setup MockApi with factory registration
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("ExternalSourcesTest_testFactoryBasedExternalSources")
    val mockApi = new MockApi(kvStoreFunc, "factory_test")

    // Register a test factory that creates handlers dynamically
    mockApi.externalRegistry.addFactory("test-factory", new TestExternalSourceFactory())

    val fetcher = mockApi.buildFetcher(true)
    fetcher.kvStore.create(ChrononMetadataKey)
    fetcher.putJoinConf(join)

    // Create test requests
    val requests = (5 until 8).map(x => Request(join.metaData.name, Map("number" -> new Integer(x))))

    val responsesF = fetcher.fetchJoin(requests)
    val responses = Await.result(responsesF, Duration(10, SECONDS))

    // Verify responses
    val numbers = mutable.HashSet.empty[Int]
    val expectedKeys = Set("ext_factory_factory_plus_two_number")

    responses.map(_.values).foreach { m =>
      assertTrue(m.isSuccess)
      assertEquals(expectedKeys, m.get.keysIterator.toSet)
      numbers.add(m.get("ext_factory_factory_plus_two_number").asInstanceOf[Int])
    }

    // Verify that factory-created handler correctly incremented numbers (5->7, 6->8, 7->9)
    assertEquals(numbers, (7 until 10).toSet)
  }

  @Test
  def testExternalSourceWithOfflineGroupBy(): Unit = {
    // Create an offline GroupBy for the external source
    val offlineGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "score" -> "score"),
            timeColumn = "ts"
          ),
          table = "offline_table"
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "score",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "offline_gb", namespace = "test"),
      accuracy = Accuracy.SNAPSHOT
    )

    // Create a regular GroupBy for comparison
    val regularGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "activity_count" -> "activity_count"),
            timeColumn = "ts"
          ),
          table = "regular_table"
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "activity_count",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "regular_gb", namespace = "test"),
      accuracy = Accuracy.SNAPSHOT
    )

    // Create factory configuration
    val factoryConfig = new ExternalSourceFactoryConfig()
    factoryConfig.setFactoryName("test-online-factory")
    factoryConfig.setFactoryParams(Map("multiplier" -> "10").toJava)

    // Create external source WITH both offlineGroupBy and factory config
    val externalSourceWithOffline = Builders.ExternalSource(
      metadata = Builders.MetaData(name = "external_with_offline"),
      keySchema = StructType("keys", Array(StructField("user_id", StringType))),
      valueSchema = StructType("values", Array(StructField("score", LongType)))
    )
    externalSourceWithOffline.setOfflineGroupBy(offlineGroupBy)
    externalSourceWithOffline.setFactoryConfig(factoryConfig)

    val namespace = "offline_test"
    val join = Builders.Join(
      left = Builders.Source.events(
        Builders.Query(selects = Map("user_id" -> "user_id")),
        table = "non_existent_table"
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = regularGroupBy,
          prefix = "regular"
        )
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          externalSourceWithOffline,
          prefix = "offline"
        )
      ),
      metaData = Builders.MetaData(name = "test/offline_join", namespace = namespace, team = "chronon")
    )

    // Setup MockApi with factory registration
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("ExternalSourcesTest_testExternalSourceWithOfflineGroupBy")
    val mockApi = new MockApi(kvStoreFunc, "offline_test")

    // Register a test factory that returns specific values
    // This proves online fetching uses the factory, NOT the offline GroupBy
    mockApi.externalRegistry.addFactory("test-online-factory", new TestOnlineFactory())

    val fetcher = mockApi.buildFetcher(true)
    fetcher.kvStore.create(ChrononMetadataKey)
    fetcher.putJoinConf(join)

    // Create test requests
    val requests = Seq(
      Request(join.metaData.name, Map("user_id" -> "user_1")),
      Request(join.metaData.name, Map("user_id" -> "user_2"))
    )

    val responsesF = fetcher.fetchJoin(requests)
    val responses = Await.result(responsesF, Duration(10, SECONDS))

    // Verify responses came from the factory for external source, not from offline GroupBy
    // This is the key test: even though offlineGroupBy is configured, online serving uses the factory
    responses.foreach { response =>
      assertTrue("Response should be successful", response.values.isSuccess)
      val responseMap = response.values.get
      val keys = responseMap.keysIterator.toSet

      // Should have external source column from factory
      assertTrue("Should contain external source column", keys.contains("ext_offline_external_with_offline_score"))

      // Should have regular GroupBy column
      assertTrue("Should contain regular GroupBy column", keys.exists(_.startsWith("regular_")))

      // Verify external source data comes from factory (100 or 200)
      // This is the core assertion: values come from ExternalSourceFactory, NOT from offlineGroupBy
      val score = responseMap("ext_offline_external_with_offline_score").asInstanceOf[Long]
      assertTrue("Score should be from factory (100 or 200), proving online uses factory not offlineGroupBy",
                 score == 100L || score == 200L)
    }

    // Verify both users got their expected scores from the factory
    val scores = responses.map(_.values.get("ext_offline_external_with_offline_score").asInstanceOf[Long]).toSet
    assertEquals("Both factory-generated scores should be present", Set(100L, 200L), scores)

    // Additional verification: Confirm the join has both regular GroupBy and external parts
    assertEquals("Join should have 1 regular join part", 1, join.joinParts.size())
    assertEquals("Join should have 1 external part", 1, join.onlineExternalParts.size())

    // Verify the external part has offlineGroupBy configured
    val externalPart = join.onlineExternalParts.get(0)
    assertNotNull("External source should have offlineGroupBy", externalPart.source.offlineGroupBy)
    assertNotNull("External source should have factory config", externalPart.source.factoryConfig)
  }

  // Test factory implementation that returns different values than what offline GroupBy would produce
  class TestOnlineFactory extends ai.chronon.online.ExternalSourceFactory {
    import ai.chronon.online.Fetcher.{Request, Response}
    import scala.concurrent.Future
    import scala.util.Success

    override def createExternalSourceHandler(
        externalSource: ai.chronon.api.ExternalSource): ai.chronon.online.ExternalSourceHandler = {
      new ai.chronon.online.ExternalSourceHandler {
        override def fetch(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
          val responses = requests.map { request =>
            val userId = request.keys("user_id").asInstanceOf[String]
            // Return deterministic values based on user_id that would be different from offline GroupBy
            val score = userId match {
              case "user_1" => 100L
              case "user_2" => 200L
              case _        => 999L
            }
            val result: Map[String, AnyRef] = Map("score" -> Long.box(score))
            Response(request = request, values = Success(result))
          }
          Future.successful(responses)
        }
      }
    }
  }

  // Test factory implementation for the factory-based registration test
  class TestExternalSourceFactory extends ai.chronon.online.ExternalSourceFactory {
    import ai.chronon.online.Fetcher.{Request, Response}
    import scala.concurrent.Future
    import scala.util.{Success, Try}

    override def createExternalSourceHandler(
        externalSource: ai.chronon.api.ExternalSource): ai.chronon.online.ExternalSourceHandler = {
      new ai.chronon.online.ExternalSourceHandler {
        override def fetch(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
          val increment = externalSource.getFactoryConfig.getFactoryParams.get("increment").toInt
          val responses = requests.map { request =>
            val number = request.keys("number").asInstanceOf[Int]
            val result: Map[String, AnyRef] = Map("number" -> Integer.valueOf(number + increment))
            Response(request = request, values = Success(result))
          }
          Future.successful(responses)
        }
      }
    }
  }
}
