package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.{MetadataStore, ModelBackend, RunModelInferenceRequest, RunModelInferenceResponse}
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}
import scala.util.Random

private case class MockModelBackend(defaultValueMap: Map[String, AnyRef]) extends ModelBackend {

  override def runModelInference(
      runModelInferenceRequest: RunModelInferenceRequest): Future[RunModelInferenceResponse] = {
    Future.successful(
      RunModelInferenceResponse(
        runModelInferenceRequest,
        runModelInferenceRequest.inputs.map { _ => defaultValueMap }
      )
    )
  }
}

class ModelTransformsTest {

  private def createSparkSession(): SparkSession =
    SparkSessionBuilder.build("ModelTransformsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
  @Test
  def testModelTransformSimple(): Unit = {
    val spark = createSparkSession()
    val tableUtils = TableUtils(spark)
    val namespace = "test_model_transforms_simple"
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("testModelTransformSimple")
    val inMemoryKvStore = kvStoreFunc()
    val defaultEmbedding = Array(0.1, 0.2, 0.3, 0.4)
    val defaultMap = Map("embedding" -> defaultEmbedding.asInstanceOf[AnyRef])
    val mockModelBackend = MockModelBackend(defaultMap)
    val mockApi = new MockApi(kvStoreFunc, namespace, mockModelBackend)
    lazy val fetcher = mockApi.buildFetcher()
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)

    tableUtils.createDatabase(namespace)
    val itemSchema = List(
      Column("item", LongType, 100),
      Column("description", api.StringType, 100)
    )

    /* prepare groupBy */
    val itemsTableName = namespace + ".item"
    val itemsDf = DataFrameGen
      .entities(spark, itemSchema, 20, partitions = 5)
      .dropDuplicates("item")
    val endDs = itemsDf.select(max("ds")).head.getString(0)
    itemsDf.save(itemsTableName)

    val itemsGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(
          query = Builders.Query(
            selects = Builders.Selects("item", "description")
          ),
          snapshotTable = itemsTableName
        )),
      keyColumns = Seq("item"),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = "unit_test/test_model_transforms_simple_items",
                                   namespace = namespace,
                                   team = "chronon")
    )
    OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, endDs, itemsGroupBy)

    /* prepare ModelTransform */
    val modelTransforms = Builders.ModelTransforms(
      transforms = Seq(
        Builders.ModelTransform(
          model = Builders.Model(
            metaData = Builders.MetaData(name = "unit_test/test_model_transforms_simple_model",
                                         namespace = namespace,
                                         team = "chronon"),
            inferenceSpec = Builders.InferenceSpec(modelBackend = "mock_model_backend"),
            inputSchema = StructType("input_schema", Array(StructField("text", StringType))),
            outputSchema = StructType("output_schema", Array(StructField("embedding", ListType(DoubleType))))
          ),
          inputMappings = Map("text" -> "unit_test_test_model_transforms_simple_items_description")
        )
      )
    )

    /* prepare Join */
    val joinConf = Builders.Join(
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = itemsGroupBy
        )
      ),
      modelTransforms = modelTransforms,
      metaData =
        Builders.MetaData(name = "unit_test/test_model_transforms_simple", namespace = namespace, team = "chronon")
    )
    metadataStore.putJoinConf(joinConf)

    /* test fetch */
    val sampleKey = itemsDf.select(col("item")).head.getLong(0)
    val joinRequest = Request(joinConf.metaData.name, Map("item" -> sampleKey.asInstanceOf[AnyRef]))
    val fetchFuture = fetcher.fetchJoin(Seq(joinRequest))
    val fetchResult = Await.result(fetchFuture, Duration(10, SECONDS))
    assertTrue(fetchResult.head.values.isSuccess)
    assertEquals(fetchResult.head.values.get, defaultMap)
  }

}
