package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.{ModelBackend, RunModelInferenceRequest, RunModelInferenceResponse}
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
    if (runModelInferenceRequest.inputs.exists(i => !i.contains("text") || i("text") == null)) {
      throw new IllegalArgumentException("Input text cannot be null")
    }

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

  private def prepareRawData(namespace: String) = {
    val spark = createSparkSession()
    val tableUtils = TableUtils(spark)

    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("testModelTransformSimple")
    val inMemoryKvStore = kvStoreFunc()
    val defaultEmbedding = Array(0.1, 0.2, 0.3, 0.4)
    val defaultMap = Map("embedding" -> defaultEmbedding.asInstanceOf[AnyRef])
    val mockModelBackend = MockModelBackend(defaultMap)
    val mockApi = new MockApi(kvStoreFunc, namespace, mockModelBackend)
    lazy val fetcher = mockApi.buildFetcher()
    inMemoryKvStore.create(ChrononMetadataKey)

    tableUtils.createDatabase(namespace)
    val itemSchema = List(
      Column("item", LongType, 100),
      Column("description", api.StringType, 100)
    )

    /* prepare groupBy */
    val itemsTableName = namespace + ".item"
    val itemsDf = DataFrameGen
      .entities(spark, itemSchema, 100, partitions = 1)
      .where(col("item").isNotNull and col("description").isNotNull)
      .dropDuplicates("item")
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
    val dataDs = itemsDf.select(max("ds")).head.getString(0)
    val servingDs = tableUtils.partitionSpec.after(dataDs)
    OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, servingDs, itemsGroupBy)

    /* prepare Model */
    val model = Builders.Model(
      metaData = Builders.MetaData(name = "unit_test/test_model_transforms_simple_model",
                                   namespace = namespace,
                                   team = "chronon"),
      inferenceSpec = Builders.InferenceSpec(modelBackend = "mock_model_backend"),
      inputSchema = StructType("input_schema", Array(StructField("text", StringType))),
      outputSchema = StructType("output_schema", Array(StructField("embedding", ListType(DoubleType))))
    )

    (itemsGroupBy, itemsDf, model, fetcher, defaultEmbedding)
  }
  @Test
  def testModelTransformSimple(): Unit = {
    val namespace = "test_model_transforms_simple"
    val (itemsGroupBy, itemsDf, model, fetcher, defaultEmbedding) = prepareRawData(namespace)

    /* prepare ModelTransform */
    val modelTransforms = Builders.ModelTransforms(
      transforms = Seq(
        Builders.ModelTransform(
          model = model,
          inputMappings = Map("text" -> "unit_test_test_model_transforms_simple_items_description"),
          outputMappings = Map("embedding" -> "description_embedding")
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
    fetcher.putJoinConf(joinConf)

    /* test fetch */
    val sampleKey = itemsDf.select(col("item")).head.getLong(0)
    val joinRequest = Request(joinConf.metaData.nameToFilePath, Map("item" -> sampleKey.asInstanceOf[AnyRef]))
    val fetchFuture = fetcher.fetchJoin(Seq(joinRequest))
    val fetchResult = Await.result(fetchFuture, Duration(10, SECONDS))
    assertTrue(fetchResult.head.values.isSuccess)

    val expectedOutputMap = Map("description_embedding" -> defaultEmbedding.asInstanceOf[AnyRef])
    assertEquals(fetchResult.head.values.get, expectedOutputMap)
  }

  @Test
  def testModelTransformMultiple(): Unit = {
    val namespace = "test_model_transforms_multiple"
    val (itemsGroupBy, itemsDf, model, fetcher, defaultEmbedding) = prepareRawData(namespace)

    /* prepare ModelTransform */
    val modelTransforms = Builders.ModelTransforms(
      transforms = Seq(
        Builders.ModelTransform(
          model = model,
          inputMappings = Map("text" -> "a_unit_test_test_model_transforms_simple_items_description"),
          outputMappings = Map("embedding" -> "description_embedding"),
          prefix = "a"
        ),
        Builders.ModelTransform(
          model = model,
          inputMappings = Map("text" -> "b_unit_test_test_model_transforms_simple_items_description"),
          outputMappings = Map("embedding" -> "description_embedding"),
          prefix = "b"
        )
      )
    )

    /* prepare Join */
    val joinConf = Builders.Join(
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = itemsGroupBy,
          prefix = "a",
          keyMapping = Map("item_a" -> "item")
        ),
        Builders.JoinPart(
          groupBy = itemsGroupBy,
          prefix = "b",
          keyMapping = Map("item_b" -> "item")
        )
      ),
      modelTransforms = modelTransforms,
      metaData =
        Builders.MetaData(name = "unit_test/test_model_transforms_multiple", namespace = namespace, team = "chronon")
    )
    fetcher.putJoinConf(joinConf)
    /* test fetch */
    val sampleKey = itemsDf.select(col("item")).take(2).map(_.getLong(0))
    val joinRequest = Request(
      joinConf.metaData.name,
      Map(
        "item_a" -> sampleKey(0).asInstanceOf[AnyRef],
        "item_b" -> sampleKey(1).asInstanceOf[AnyRef]
      )
    )
    val fetchFuture = fetcher.fetchJoin(Seq(joinRequest))
    val fetchResult = Await.result(fetchFuture, Duration.Inf)
    assertTrue(fetchResult.head.values.isSuccess)
    val expectedOutputMap = Map(
      "a_description_embedding" -> defaultEmbedding.asInstanceOf[AnyRef],
      "b_description_embedding" -> defaultEmbedding.asInstanceOf[AnyRef]
    )
    assertEquals(fetchResult.head.values.get, expectedOutputMap)
  }

}
