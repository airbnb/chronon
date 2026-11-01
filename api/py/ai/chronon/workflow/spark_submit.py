from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from ai.chronon.repo.validator import Validator
from thrift.protocol import TJSONProtocol
from thrift.transport import TTransport


class ChrononSparkSubmitOperator(SparkSubmitOperator):
    """
    Custom SparkSubmitOperator for Chronon that:
    - Validates Chronon job configuration.
    - Serializes the input Thrift object to JSON.
    - Submits the Spark job.
    """

    def __init__(self, thrift_obj, *args, **kwargs):
        """
        :param thrift_obj: A Chronon Thrift object (e.g., GroupBy, Join, Staging).
        """
        self.thrift_obj = thrift_obj
        super().__init__(*args, **kwargs)

    def validate_config(self):
        """
        Validates the Chronon Thrift object using Chronon's Validator.
        """
        validator = Validator()
        errors = validator.validate(self.thrift_obj)

        if errors:
            raise ValueError(f"Chronon config validation failed: {errors}")

    def serialize_to_json(self):
        """
        Serializes the Chronon Thrift object to JSON for Spark.
        """
        transport = TTransport.TMemoryBuffer()
        protocol = TJSONProtocol.TJSONProtocol(transport)
        self.thrift_obj.write(protocol)
        return transport.getvalue().decode("utf-8")

    def execute(self, context):
        """
        Overrides SparkSubmitOperator execute method:
        1. Validate the Thrift object.
        2. Serialize it to JSON.
        3. Pass JSON to SparkSubmitOperator.
        """
        self.log.info("Validating Chronon Thrift object...")
        self.validate_config()

        self.log.info("Serializing Thrift object to JSON...")
        serialized_json = self.serialize_to_json()

        # Pass serialized JSON as an argument to the Spark job
        self._application_args = [serialized_json]

        self.log.info("Submitting Spark job with Chronon config...")
        super().execute(context)
