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
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("external_test")
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
}
