package ai.chronon.spark
import ai.chronon.api.Extensions.{JoinOps, ModelTransformOps}
import ai.chronon.api.ModelTransform
import ai.chronon.online.ModelBackend
import org.apache.spark.sql.SparkSession
case class ModelTransformBatchJob(
    sparkSession: SparkSession,
    modelBackend: ModelBackend,
    join: ai.chronon.api.Join,
    startPartition: String,
    endPartition: String,
    modelTransformOverride: Option[String]
) {

  private def getModelTransformOverride: Option[ModelTransform] = {
    modelTransformOverride.flatMap(modelTransformName =>
      join.modelTransformsListScala.find { modelTransform: ModelTransform =>
        modelTransform.name == modelTransformName
      })
  }
  def run(): Unit = {
    modelBackend.runModelInferenceBatchJob(
      sparkSession,
      join,
      startPartition,
      endPartition,
      getModelTransformOverride
    )
  }
}
