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

package ai.chronon.online

import ai.chronon.api._
import ai.chronon.online.Fetcher.{Request, Response, StructuredResponse}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}
import org.mockito.Answers
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import java.util
import scala.collection.Seq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

/**
  * Covers the structured fetch entry points. Both wrap the classic map-returning fetch and re-key
  * struct-typed values by name, so these tests stub the underlying fetch and the schema lookup and
  * assert on the shape of the resulting FeatureRecord.
  */
class FetcherStructuredTest extends MockitoSugar with MockitoHelper {

  private val GroupByName = "unit_test.listing_signals"
  private val JoinName = "unit_test/listing_join"

  private val contentSchema =
    StructType("Content", Array(StructField("kind", StringType), StructField("text", StringType)))
  private val insightSchema =
    StructType("Insight", Array(StructField("summary", StringType), StructField("contents", ListType(contentSchema))))

  private var kvStore: KVStore = _

  @Before
  def setup(): Unit = {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
  }

  // A nested struct value in the positional shape AvroConversions.toChrononRow produces.
  private def positionalInsight(summary: String, texts: Seq[String]): Array[Any] = {
    val contents = new util.ArrayList[Any]()
    texts.foreach(t => contents.add(Array[Any]("HIGHLIGHT", t)))
    Array[Any](summary, contents)
  }

  private def servingInfoWith(schema: StructType): GroupByServingInfoParsed = {
    val servingInfo = mock[GroupByServingInfoParsed]
    when(servingInfo.responseChrononSchema).thenReturn(schema)
    servingInfo
  }

  private def groupByFetcher(schema: StructType, values: Try[Map[String, AnyRef]]): FetcherBase =
    new FetcherBase(kvStore) {
      override lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
        new TTLCache[String, Try[GroupByServingInfoParsed]]({ _: String => Success(servingInfoWith(schema)) },
                                                            { name: String =>
                                                              Metrics.Context(environment = "test", groupBy = name)
                                                            })

      override def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] =
        Future.successful(requests.map(r => Response(r, values)))
    }

  private def joinFetcher(schema: StructType, values: Try[Map[String, AnyRef]]): Fetcher =
    new Fetcher(kvStore = kvStore, metaDataSet = "test_metadata") {
      override lazy val getJoinCodecs: TTLCache[String, Try[(JoinCodec, Boolean)]] =
        new TTLCache[String, Try[(JoinCodec, Boolean)]](
          { _: String =>
            val codec = mock[JoinCodec]
            when(codec.valueSchema).thenReturn(schema)
            Success((codec, false))
          },
          { name: String => Metrics.Context(environment = "test", join = name) }
        )

      override def fetchJoin(requests: Seq[Request], joinConf: Option[Join] = None): Future[Seq[Response]] =
        Future.successful(requests.map(r => Response(r, values)))
    }

  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  @Test
  def fetchGroupByStructuredNamesNestedStructs(): Unit = {
    val schema = StructType("GbValue", Array(StructField("count", LongType), StructField("insight", insightSchema)))
    val values: Map[String, AnyRef] =
      Map("count" -> Long.box(7L), "insight" -> positionalInsight("looks good", Seq("clean", "quiet")))

    val fetcher = groupByFetcher(schema, Success(values))
    val responses = await(fetcher.fetchGroupByStructured(Seq(Request(GroupByName, Map("listing" -> Long.box(1L))))))

    assertEquals(1, responses.size)
    val record = responses.head.values.get
    assertEquals(7L, record.getLong("count"))

    val insight = record.getStruct("insight")
    assertEquals("looks good", insight.getString("summary"))
    val contents = insight.getStructList("contents")
    assertEquals(2, contents.size)
    assertEquals("clean", contents.head.getString("text"))
    assertEquals("quiet", contents(1).getString("text"))
  }

  @Test
  def fetchGroupByStructuredPreservesRequests(): Unit = {
    val schema = StructType("GbValue", Array(StructField("count", LongType)))
    val fetcher = groupByFetcher(schema, Success(Map("count" -> Long.box(1L))))
    val request = Request(GroupByName, Map("listing" -> Long.box(42L)))

    val responses = await(fetcher.fetchGroupByStructured(Seq(request)))
    assertEquals(request, responses.head.request)
  }

  @Test
  def fetchGroupByStructuredSurfacesFetchFailure(): Unit = {
    val schema = StructType("GbValue", Array(StructField("count", LongType)))
    val boom = new RuntimeException("kv store exploded")
    val fetcher = groupByFetcher(schema, scala.util.Failure(boom))

    val responses = await(fetcher.fetchGroupByStructured(Seq(Request(GroupByName, Map("listing" -> Long.box(1L))))))
    assertTrue(responses.head.values.isFailure)
    assertEquals(boom, responses.head.values.failed.get)
  }

  @Test
  def fetchGroupByStructuredHandlesNullValueMap(): Unit = {
    // fetchGroupBys returns a null value map when the key is absent from the KV store.
    val schema = StructType("GbValue", Array(StructField("count", LongType)))
    val fetcher = groupByFetcher(schema, Success(null))

    val responses = await(fetcher.fetchGroupByStructured(Seq(Request(GroupByName, Map("listing" -> Long.box(1L))))))
    assertEquals(null, responses.head.values.get)
  }

  @Test
  def fetchJoinStructuredNamesNestedStructs(): Unit = {
    val schema = StructType(
      "JoinValue",
      Array(StructField("gb_count", LongType), StructField("gb_insight", insightSchema))
    )
    val values: Map[String, AnyRef] =
      Map("gb_count" -> Long.box(3L), "gb_insight" -> positionalInsight("ok", Seq("spotless")))

    val fetcher = joinFetcher(schema, Success(values))
    val responses = await(fetcher.fetchJoinStructured(Seq(Request(JoinName, Map("listing" -> Long.box(1L))))))

    assertEquals(1, responses.size)
    val record = responses.head.values.get
    assertEquals(3L, record.getLong("gb_count"))
    assertEquals("spotless", record.getStruct("gb_insight").getStructList("contents").head.getString("text"))
  }

  @Test
  def fetchJoinStructuredRetainsExceptionKeys(): Unit = {
    // A join response can carry per-join-part failures alongside good values. Those keys are not
    // in the value schema, so they must still be reachable from the structured response.
    val schema = StructType("JoinValue", Array(StructField("gb_count", LongType)))
    val values: Map[String, AnyRef] =
      Map("gb_count" -> Long.box(3L), "some_part_exception" -> "part blew up")

    val fetcher = joinFetcher(schema, Success(values))
    val record =
      await(fetcher.fetchJoinStructured(Seq(Request(JoinName, Map("listing" -> Long.box(1L)))))).head.values.get

    assertEquals(3L, record.getLong("gb_count"))
    assertTrue(record.hasErrors)
    assertEquals("part blew up", record.errors("some_part_exception"))
  }

  @Test
  def fetchJoinStructuredNarrowsSupersetSchema(): Unit = {
    // JoinCodec.valueSchema spans base and derived fields while a response holds only one set;
    // the record's schema should describe what is actually there.
    val schema =
      StructType("JoinValue", Array(StructField("base_count", LongType), StructField("derived_count", LongType)))
    val fetcher = joinFetcher(schema, Success(Map("derived_count" -> Long.box(9L))))

    val record =
      await(fetcher.fetchJoinStructured(Seq(Request(JoinName, Map("listing" -> Long.box(1L)))))).head.values.get

    assertEquals(Seq("derived_count"), record.schema.fields.map(_.name).toSeq)
    assertEquals(None, record.getLongOpt("base_count"))
    assertEquals(Some(9L), record.getLongOpt("derived_count"))
  }

  @Test
  def fetchJoinStructuredHandlesBatchOfRequests(): Unit = {
    val schema = StructType("JoinValue", Array(StructField("gb_count", LongType)))
    val fetcher = joinFetcher(schema, Success(Map("gb_count" -> Long.box(2L))))
    val requests = (1 to 5).map(i => Request(JoinName, Map("listing" -> Long.box(i.toLong))))

    val responses: Seq[StructuredResponse] = await(fetcher.fetchJoinStructured(requests))
    assertEquals(5, responses.size)
    responses.foreach(r => assertEquals(2L, r.values.get.getLong("gb_count")))
  }
}
