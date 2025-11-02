/*
 *    Copyright (C) 2025 The Chronon Authors.
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

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.{ModelBackend, RunModelInferenceRequest, RunModelInferenceResponse}
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.{ModelTransformBatchJob, SparkSessionBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, max}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.util.ScalaJavaConversions.ListOps

private case class MockModelBackend(
    defaultValueMap: Map[String, AnyRef],
    defaultEmbeddingValue: Array[Double]
) extends ModelBackend {

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

  override def runModelInferenceBatchJob(
      sparkSession: SparkSession,
      join: ai.chronon.api.Join,
      startPartition: String,
      endPartition: String,
      modelTransform: Option[ModelTransform] = None,
      jobContextJson: Option[String] = None
  ): Option[DataFrame] = {

    val embeddingColumns = join.modelSchema.fields.map(_.name).map { n =>
      lit(defaultEmbeddingValue).as(n)
    }
    val columns = join.rowIds.toScala.map(col) ++ embeddingColumns :+ col("ds")
    Some(
      sparkSession
        .table(join.metaData.preModelTransformsTable)
        .where(col("ds").between(startPartition, endPartition))
        .select(columns: _*))
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
    val mockModelBackend = MockModelBackend(defaultMap, defaultEmbedding)
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
      .entities(spark, itemSchema, 100, partitions = 5)
      .where(col("item").isNotNull and col("description").isNotNull)
      .dropDuplicates("item")
    itemsDf.save(itemsTableName)
    val maxDs = itemsDf.select(max("ds")).head.getString(0)

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

    (itemsGroupBy, itemsDf, model, fetcher, defaultEmbedding, maxDs)
  }
  @Test
  def testModelTransformSimple(): Unit = {
    val namespace = "test_model_transform_simple"
    val (itemsGroupBy, itemsDf, model, fetcher, defaultEmbedding, maxDs) = prepareRawData(namespace)

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
        Builders.MetaData(name = "unit_test/test_model_transform_simple", namespace = namespace, team = "chronon")
    )
    fetcher.putJoinConf(joinConf)

    /* test fetch */
    val sampleKey = itemsDf.select(col("item")).where(col("ds") === maxDs).head.getLong(0)
    val joinRequest = Request(joinConf.metaData.nameToFilePath, Map("item" -> sampleKey.asInstanceOf[AnyRef]))
    val fetchFuture = fetcher.fetchJoin(Seq(joinRequest))
    val fetchResult = Await.result(fetchFuture, Duration(10, SECONDS))
    assertTrue(fetchResult.head.values.isSuccess)

    val expectedOutputMap = Map("description_embedding" -> defaultEmbedding.asInstanceOf[AnyRef])
    assertEquals(fetchResult.head.values.get, expectedOutputMap)
  }

  @Test
  def testModelTransformMultiple(): Unit = {
    val namespace = "test_model_transform_multiple"
    val (itemsGroupBy, itemsDf, model, fetcher, defaultEmbedding, maxDs) = prepareRawData(namespace)

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
        Builders.MetaData(name = "unit_test/test_model_transform_multiple", namespace = namespace, team = "chronon")
    )
    fetcher.putJoinConf(joinConf)
    /* test fetch */
    val sampleKey = itemsDf.select(col("item")).where(col("ds") === maxDs).take(2).map(_.getLong(0))
    val joinRequest = Request(
      joinConf.metaData.name,
      Map(
        "item_a" -> sampleKey(0).asInstanceOf[AnyRef],
        "item_b" -> sampleKey(1).asInstanceOf[AnyRef]
      )
    )
    val fetchFuture = fetcher.fetchJoin(Seq(joinRequest, joinRequest, joinRequest))
    val fetchResult = Await.result(fetchFuture, Duration.Inf)
    assertTrue(fetchResult.head.values.isSuccess)
    val expectedOutputMap = Map(
      "a_description_embedding" -> defaultEmbedding.asInstanceOf[AnyRef],
      "b_description_embedding" -> defaultEmbedding.asInstanceOf[AnyRef]
    )
    assertEquals(fetchResult.head.values.get, expectedOutputMap)
  }

  @Test
  def testModelTransformBatchSimple(): Unit = {
    val namespace = "test_model_transform_batch_simple"
    val (itemsGroupBy, itemsDf, model, fetcher, defaultEmbedding, _) = prepareRawData(namespace)
    val spark = itemsDf.sparkSession
    val tableUtils = TableUtils(spark)
    val defaultMap = Map("embedding" -> defaultEmbedding.asInstanceOf[AnyRef])
    val mockModelBackend = MockModelBackend(defaultMap, defaultEmbedding)

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

    // left side
    val itemQueries = List(Column("item", api.LongType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .entities(spark, itemQueries, 100, partitions = 5)
      .save(itemQueriesTable)
    val endDs = tableUtils.partitions(itemQueriesTable).max

    /* prepare Join */
    val joinConf = Builders.Join(
      left = Builders.Source.entities(
        query = Builders.Query(
          selects = Builders.Selects("item")
        ),
        snapshotTable = itemQueriesTable
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = itemsGroupBy
        )
      ),
      rowIds = Seq("item"),
      modelTransforms = modelTransforms,
      metaData =
        Builders.MetaData(name = "unit_test/test_model_transform_batch_simple", namespace = namespace, team = "chronon")
    )

    /* run join job to produce pre-mt table */
    val joinJob = new ai.chronon.spark.Join(joinConf, endDs, tableUtils)
    joinJob.computeJoin()

    // run model transform batch job
    val modelTransformJob = new ModelTransformBatchJob(spark, mockModelBackend, joinConf, endDs)
    modelTransformJob.run()

    assertTrue(tableUtils.tableExists(joinConf.metaData.outputTable))
    assertEquals(5, tableUtils.partitions(joinConf.metaData.outputTable).size)
  }

}
