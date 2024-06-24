from ai.chronon.api.ttypes import GroupBy, Join, Source, Query, StagingQuery
from ai.chronon.repo.serializer import thrift_simple_json
from ai.chronon.utils import output_table_name
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from py4j.java_gateway import JavaObject, JVMView
from datetime import datetime, timedelta


class DatabricksExecutable:
    def __init__(self, spark_session: SparkSession) -> None:
        self._spark: SparkSession = spark_session
        self._jvm: JVMView = self._spark._jvm

        # Constants Provider
        self._constants_provider: JavaObject = self._jvm.ai.chronon.spark.databricks.DatabricksConstantsNameProvider()
        self._jvm.ai.chronon.api.Constants.initConstantNameProvider(self._constants_provider)

        # Table Utils
        self._table_utils: JavaObject = self._jvm.ai.chronon.spark.databricks.DatabricksTableUtils(spark_session._jsparkSession)

        # Start date / end date defaults for analyze + validate operations
        self._default_start_date = (datetime.now() - timedelta(days=8)).strftime('%Y%m%d')
        self._default_end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


    def _get_query_with_updated_start_and_end_date(self, query: Query, start_date: str, end_date: str):
        """
        Get a new query with updated start and end date.
        """
        q = query
        q.startPartition = start_date
        q.endPartition = end_date
        return q
    
    def _get_source_with_updated_start_and_end_date(self, source: Source, start_date: str, end_date: str):
        """
        Get a new source with updated start and end date.
        """
        if source.events:
            source.events.query = self._get_query_with_updated_start_and_end_date(source.events.query, start_date, end_date)
        else:
            source.entities.query = self._get_query_with_updated_start_and_end_date(source.entities.query, start_date, end_date)
        return source
    
    def _get_sources_with_updated_start_and_end_date(self, sources: list, start_date: str, end_date: str):
        """
        Get a new list of sources with updated start and end date.
        """
        for source in sources:
            source = self._get_source_with_updated_start_and_end_date(source, start_date, end_date)
        return sources

    
    def _print_with_timestamp(self, message):
        """
        We want the output to go to the cell for the notebook instead of the logs so we use print instead of a logger.
        """
        current_utc_time = datetime.utcnow()
        time_str = current_utc_time.strftime('[%Y-%m-%d %H:%M:%S UTC]')
        print(f'{time_str} {message}')

    def _drop_table_if_exists(self, table_name: str):
        """
        Every time we run a staging query or group by backfill in Databricks we will drop the output table if it exists.
        We do this to avoid any issues with the table already existing.
        We don't need to do this for joins because we have archiving setup for that code flow. 
        """
        self._print_with_timestamp(f"Dropping table {table_name} if it exists.")
        self._spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        

class DatabricksGroupBy(DatabricksExecutable):
    def __init__(self, group_by: GroupBy, spark_session: SparkSession) -> None:
        self.group_by: GroupBy = group_by
        super().__init__(spark_session)

    def _get_group_by_to_execute(self, start_date: str) -> GroupBy:
        group_by_to_execute: GroupBy = self.group_by
        group_by_to_execute.backfillStartDate = start_date
        return group_by_to_execute
    
    def _get_java_group_by(self, group_by: GroupBy, end_date: str) -> JavaObject:
        """
        Converts our Python GroupBy object to a Java GroupBy object and handles raw S3 prefixes.
        """
        java_group_by: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.parseGroupBy(
            thrift_simple_json(group_by)
        )

        java_group_by_with_updated_s3_prefixes = self._jvm.ai.chronon.spark.S3Utils.readAndUpdateS3PrefixesForGroupBy(java_group_by, end_date, self._spark._jsparkSession)
        
        return java_group_by_with_updated_s3_prefixes

    def run(self, start_date:str, end_date: str, step_days: int = 30) -> DataFrame:
        """
        Performs a GroupBy Backfill operation.
        """

        self._print_with_timestamp(f"Executing GroupBy {self.group_by.metaData.name} from {start_date} to {end_date} with step_days {step_days}")

        group_by_to_execute: GroupBy = self._get_group_by_to_execute(start_date)
        group_by_output_table: str = f"{self.group_by.metaData.outputNamespace}.{self.group_by.metaData.name}"

        self._drop_table_if_exists(group_by_output_table)

        java_group_by: JavaObject = self._get_java_group_by(group_by_to_execute, end_date)

        result_df_scala: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.runGroupBy(
            java_group_by,
            end_date,
            self._jvm.ai.chronon.spark.PySparkUtils.getIntOptional(str(step_days)),
            self._table_utils,
            self._constants_provider
        )

        self._print_with_timestamp(f"GroupBy {self.group_by.metaData.name} executed successfully and was written to iceberg.{group_by_output_table}")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
        return DataFrame(result_df_scala, self._spark)
    
    def analyze(self, start_date:str = None, end_date: str = None, enable_hitter_analysis: bool = False):
        """
        Runs the analyzer on a Groupby.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """
        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date  

        self._print_with_timestamp(f"Analyzing GroupBy {self.group_by.metaData.name} from {start_date} to {end_date}")
        self._print_with_timestamp(f"Enable Hitter Analysis: {enable_hitter_analysis}")

        group_by_to_analyze: GroupBy = self._get_group_by_to_execute(start_date)

        java_group_by: JavaObject = self._get_java_group_by(group_by_to_analyze, end_date)

        self._jvm.ai.chronon.spark.PySparkUtils.analyzeGroupBy(
            java_group_by,
            start_date, 
            end_date, 
            enable_hitter_analysis, 
            self._table_utils,
            self._constants_provider
        )

        self._print_with_timestamp(f"GroupBy {self.group_by.metaData.name} analyzed successfully.")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
    
    def validate(self, start_date:str = None, end_date: str = None):
        """
        Runs the validator on a Groupby.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """
        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date

        self._print_with_timestamp(f"Validating GroupBy {self.group_by.metaData.name} from {start_date} to {end_date}")

        group_by_to_validate: GroupBy = self._get_group_by_to_execute(start_date)

        java_group_by: JavaObject = self._get_java_group_by(group_by_to_validate, end_date)

        errors_list: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.validateGroupBy(
            java_group_by, 
            start_date, 
            end_date, 
            self._table_utils,
            self._constants_provider
        )

        if errors_list.length() > 0:
            self._print_with_timestamp("Validation failed for GroupBy with the following errors:")
            self._print_with_timestamp(errors_list)
        else:
            self._print_with_timestamp("Validation passed for GroupBy.")

        self._print_with_timestamp(f"Validation for GroupBy {self.group_by.metaData.name} has completed.")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
        

class DatabricksJoin(DatabricksExecutable):
    def __init__(self, join: Join, spark_session: SparkSession) -> None:
        self.join: Join = join
        super().__init__(spark_session)
    
    def _get_join_to_execute(self, start_date: str, end_date: str) -> Join:
        join_to_execute: Join = self.join
        join_to_execute.left = self._get_source_with_updated_start_and_end_date(join_to_execute.left, start_date, end_date)
        return join_to_execute
    
    def _get_java_join(self, join: Join, end_date: str) -> JavaObject:
        """
        Converts our Python Join object to a Java Join object and handles raw S3 prefixes.
        """
        java_join: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.parseJoin(
            thrift_simple_json(join)
        )

        java_join_with_updated_s3_prefixes = self._jvm.ai.chronon.spark.S3Utils.readAndUpdateS3PrefixesForJoin(java_join, end_date, self._spark._jsparkSession)
        
        return java_join_with_updated_s3_prefixes
    
    def run(self, start_date:str, end_date: str, step_days: int = 30, skip_first_hole: bool = False, sample_num_of_rows: int = None) -> DataFrame:
        """
        Performs a Join Backfill operation.
        """
        self._print_with_timestamp(f"Executing Join {self.join.metaData.name} from {start_date} to {end_date} with step_days {step_days}")
        self._print_with_timestamp(f"Skip First Hole: {skip_first_hole}")
        self._print_with_timestamp(f"Sample Number of Rows: {sample_num_of_rows}")

        join_to_execute: Join = self._get_join_to_execute(start_date, end_date)

        java_join: JavaObject = self._get_java_join(join_to_execute, end_date)

        result_df_scala = self._jvm.ai.chronon.spark.PySparkUtils.runJoin(
            java_join,
            end_date,
            self._jvm.ai.chronon.spark.PySparkUtils.getIntOptional(None if not step_days else str(step_days)),
            skip_first_hole,
            self._jvm.ai.chronon.spark.PySparkUtils.getIntOptional(None if not sample_num_of_rows else str(sample_num_of_rows)),
            self._table_utils,
            self._constants_provider
        )

        self._print_with_timestamp("The following intermediate tables were created or written to as part of this join:")
        for jp in self.join.joinParts:
            self._print_with_timestamp(f"Intermediate table: iceberg.{jp.groupBy.metaData.outputNamespace}.{self.join.metaData.name}_{jp.groupBy.metaData.name}")
        self._print_with_timestamp(f"Join {self.join.metaData.name} executed successfully and was written to iceberg.{self.join.metaData.outputNamespace}.{self.join.metaData.name}.")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
        return DataFrame(result_df_scala, self._spark)

    def analyze(self, start_date:str = None, end_date: str = None, enable_hitter_analysis: bool = False):
        """
        Runs the analyzer on a Join.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """

        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date

        self._print_with_timestamp(f"Analyzing Join {self.join.metaData.name} from {start_date} to {end_date}")
        self._print_with_timestamp(f"Enable Hitter Analysis: {enable_hitter_analysis}")

        join_to_analyze: Join = self._get_join_to_execute(start_date, end_date)

        java_join: JavaObject = self._get_java_join(join_to_analyze, end_date)

        self._jvm.ai.chronon.spark.PySparkUtils.analyzeJoin(
            java_join,
            start_date, 
            end_date, 
            enable_hitter_analysis, 
            self._table_utils,
            self._constants_provider
        )

        self._print_with_timestamp(f"Join {self.join.metaData.name} analyzed successfully.")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
    
    def validate(self, start_date:str = None, end_date: str = None):
        """
        Runs the validator on a Join.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """

        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date

        self._print_with_timestamp(f"Validating Join {self.join.metaData.name} from {start_date} to {end_date}")

        join_to_validate: Join = self._get_join_to_execute(start_date, end_date)

        java_join: JavaObject = self._get_java_join(join_to_validate, end_date)

        errors_list: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.validateJoin(
            java_join, 
            start_date, 
            end_date, 
            self._table_utils,
            self._constants_provider
        )

        if errors_list.length() > 0:
            self._print_with_timestamp("Validation failed for Join with the following errors:")
            self._print_with_timestamp(errors_list)
        else:
            self._print_with_timestamp("Validation passed for Join.")
        
        self._print_with_timestamp(f"Validation for Join {self.join.metaData.name} has completed.")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")

class DatabricksStagingQuery(DatabricksExecutable):
    def __init__(self, staging_query: StagingQuery, spark_session: SparkSession) -> None:
        assert staging_query.metaData.name, "Staging Query name is required."
        assert staging_query.metaData.outputNamespace, "Staging Query outputNamespace is required and must be set to `chronon_poc_usertables`."


        self.staging_query: StagingQuery = staging_query
        super().__init__(spark_session)

    def _get_staging_query_to_execute(self, start_date: str) -> StagingQuery:
        staging_query_to_execute: StagingQuery = self.staging_query
        staging_query_to_execute.startPartition = start_date
        return staging_query_to_execute
    
    def run(self, start_date:str, end_date: str, step_days: int = 30, skip_first_hole: bool = False) -> DataFrame:
        """
        Executes a Staging Query.
        """
        self._print_with_timestamp(f"Executing Staging Query {self.staging_query.metaData.name} from {start_date} to {end_date}")
        self._print_with_timestamp(f"Skip First Hole: {skip_first_hole}")


        staging_query_to_execute: StagingQuery = self._get_staging_query_to_execute(start_date)
        staging_query_output_table_name = output_table_name(staging_query_to_execute, full_name=True)

        self._drop_table_if_exists(staging_query_output_table_name)
        
        java_staging_query: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.parseStagingQuery(
            thrift_simple_json(staging_query_to_execute)
        )

        self._jvm.ai.chronon.spark.PySparkUtils.runStagingQuery(
            java_staging_query,
            end_date,
            self._jvm.ai.chronon.spark.PySparkUtils.getIntOptional(None if not step_days else str(step_days)),
            skip_first_hole,
            self._table_utils,
            self._constants_provider
        )


        result_df = self._spark.sql(f"SELECT * FROM {staging_query_output_table_name}")

        self._print_with_timestamp(f"Staging Query {self.staging_query.metaData.name} executed successfully and was written to {staging_query_output_table_name} .")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
        return result_df