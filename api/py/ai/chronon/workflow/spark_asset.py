from dagster import asset, AssetExecutionContext
from dagster_spark import SparkSubmitTaskDefinition
from thrift.protocol import TJSONProtocol
from thrift.transport import TTransport
from ai.chronon.repo.validator import Validator

def serialize_thrift_to_json(thrift_obj):
    """
    Serializes a Thrift object to JSON for Spark.
    """
    transport = TTransport.TMemoryBuffer()
    protocol = TJSONProtocol.TJSONProtocol(transport)
    thrift_obj.write(protocol)
    return transport.getvalue().decode("utf-8")

def validate_thrift_obj(thrift_obj):
    """
    Validates the Chronon Thrift object using Chronon's Validator.
    """
    validator = Validator()
    errors = validator.validate(thrift_obj)
    if errors:
        raise ValueError(f"Chronon config validation failed: {errors}")

@asset
def chronon_spark_job(context: AssetExecutionContext, thrift_obj):
    """
    Dagster asset that:
    - Validates a Chronon Thrift object.
    - Serializes it to JSON.
    - Submits a Spark job using SparkSubmitTaskDefinition.
    """

    # Step 1: Validate
    context.log.info("Validating Chronon Thrift object...")
    validate_thrift_obj(thrift_obj)

    # Step 2: Serialize to JSON
    context.log.info("Serializing Thrift object to JSON...")
    serialized_json = serialize_thrift_to_json(thrift_obj)

    # Step 3: Submit Spark job
    context.log.info("Submitting Spark job...")
    spark_task = SparkSubmitTaskDefinition(
        name="chronon_spark_submit",
        application="ai.chronon:spark_uber_2.12:0.0.8",
        application_args=[serialized_json],
        spark_conf={
            "spark.jars.packages": "ai.chronon:spark_uber_2.12:0.0.8",
        }
    )

    return spark_task.execute(context)
