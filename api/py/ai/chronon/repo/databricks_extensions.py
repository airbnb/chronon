from ai.chronon.api.ttypes import GroupBy, Join, Source, Query, StagingQuery
from ai.chronon.repo.serializer import thrift_simple_json
from ai.chronon.utils import output_table_name, set_name, print_logs_in_cell
from ai.chronon.repo import NOTEBOOKS_OUTPUT_NAMESPACE

from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from py4j.java_gateway import JavaObject, JVMView
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
import re
import tempfile
import os
import re


class DatabricksExecutable:
    def __init__(self, spark_session: SparkSession) -> None:
        self._spark: SparkSession = spark_session
        self._jvm: JVMView = self._spark._jvm

        # Databricks Utils 
        self._dbutils = DBUtils(self._spark)

        # Constants Provider
        self._constants_provider: JavaObject = self._jvm.ai.chronon.spark.databricks.DatabricksConstantsNameProvider()
        self._jvm.ai.chronon.api.Constants.initConstantNameProvider(self._constants_provider)

        # Table Utils
        self._table_utils: JavaObject = self._jvm.ai.chronon.spark.databricks.DatabricksTableUtils(spark_session._jsparkSession)

        # Start date / end date defaults for analyze + validate operations
        self._default_start_date = (datetime.now() - timedelta(days=8)).strftime('%Y%m%d')
        self._default_end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    def _get_databricks_user(self):
        user_email = self._dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        return user_email.split('@')[0].lower()
    
    def _remove_non_table_name_friendly_characters_from_notebook_name(self, notebook_name):
        notebook_name_with_spaces_replaced = notebook_name.replace(" ", "_")

        # Friendly characters: a-z, A-Z, 0-9, _
        friendly_pattern = re.compile(r'[^a-zA-Z0-9_]')
        cleansed_notebook_name = friendly_pattern.sub('', notebook_name_with_spaces_replaced)
        
        return cleansed_notebook_name

    def _get_databricks_notebook_name(self):
        full_notebook_path = self._dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        uncleansed_notebook_name = full_notebook_path.split("/")[-1]
        return self._remove_non_table_name_friendly_characters_from_notebook_name(uncleansed_notebook_name)

    def _set_metadata(self, obj):
        obj_type = type(obj)
        name_prefix = f"{self._get_databricks_user()}.{self._get_databricks_notebook_name()}"
        
        # Note: name is what is used to determine the output table names
        # If the user is executing an object that is from zoolander, we will derive the name from the file path
        # If they defined the feature in cell we will require them to provide a name
        # In addition to this, we should always aim to prefix the name with the user and notebook name
        # This will prevent two users from overwriting each other's prototyping work
        if not obj.metaData.name:
            try:
                # Make an attempt to see if we can set the name attribute of the object
                # src here refers to the src folder in zoolander
                set_name(obj, obj_type, "src")
            except AttributeError:
                # If we can't determine the name using the garbage collector, that indicates that this feature was defined in cell where name should already have been provided.
                raise AttributeError("Please provide a name when defining group_bys/joins/staging_queries in a notebook cell. This can be done via the metaData.name attribute.")
        else:
            # Avoid adding the prefix multiple times
            obj.metaData.name = obj.metaData.name.replace(f"{name_prefix}_", "")

        obj.metaData.outputNamespace = NOTEBOOKS_OUTPUT_NAMESPACE
        
        if obj_type == Join:
            for jp in obj.joinParts:
                jp.groupBy.metaData.outputNamespace = NOTEBOOKS_OUTPUT_NAMESPACE
        
        obj.metaData.name = f"{name_prefix}_{obj.metaData.name}"

        return obj

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
        super().__init__(spark_session)
        self.group_by: GroupBy = self._set_metadata(group_by)


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

    @print_logs_in_cell
    def run(self, start_date:str, end_date: str, step_days: int = 30) -> DataFrame:
        """
        Performs a GroupBy Backfill operation.
        """

        self._print_with_timestamp(f"Executing GroupBy {self.group_by.metaData.name} from {start_date} to {end_date} with step_days {step_days}")

        group_by_to_execute: GroupBy = self._get_group_by_to_execute(start_date)
        group_by_output_table: str = output_table_name(group_by_to_execute, full_name=True)

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
        self._print_with_timestamp("Job Logs: \n\n")
        return DataFrame(result_df_scala, self._spark)
    
    @print_logs_in_cell
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
        self._print_with_timestamp("Job Logs: \n\n")
    
    @print_logs_in_cell
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
        self._print_with_timestamp("Job Logs: \n\n")
        

class DatabricksJoin(DatabricksExecutable):
    def __init__(self, join: Join, spark_session: SparkSession) -> None:
        super().__init__(spark_session)
        self.join: Join = self._set_metadata(join)
    
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
    
    @print_logs_in_cell
    def run(self, start_date:str, end_date: str, step_days: int = 30, skip_first_hole: bool = False, sample_num_of_rows: int = None) -> DataFrame:
        """
        Performs a Join Backfill operation.
        """
        self._print_with_timestamp(f"Executing Join {self.join.metaData.name} from {start_date} to {end_date} with step_days {step_days}")
        self._print_with_timestamp(f"Skip First Hole: {skip_first_hole}")
        self._print_with_timestamp(f"Sample Number of Rows: {sample_num_of_rows}")

        join_to_execute: Join = self._get_join_to_execute(start_date, end_date)
        join_output_table: str = output_table_name(join_to_execute, full_name=True)

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

        self._print_with_timestamp(f"Join {self.join.metaData.name} executed successfully and was written to iceberg.{join_output_table}")
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
        self._print_with_timestamp("Job Logs: \n\n")
        return DataFrame(result_df_scala, self._spark)

    @print_logs_in_cell 
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
        self._print_with_timestamp("Job Logs: \n\n")
    
    @print_logs_in_cell
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
        self._print_with_timestamp("Job Logs: \n\n")

class DatabricksStagingQuery(DatabricksExecutable):
    def __init__(self, staging_query: StagingQuery, spark_session: SparkSession) -> None:
        super().__init__(spark_session)
        self.staging_query: StagingQuery = self._set_metadata(staging_query)

    def _get_staging_query_to_execute(self, start_date: str) -> StagingQuery:
        staging_query_to_execute: StagingQuery = self.staging_query
        staging_query_to_execute.startPartition = start_date
        return staging_query_to_execute
    
    @print_logs_in_cell
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

        self._print_with_timestamp(f"Staging Query {self.staging_query.metaData.name} executed successfully and was written to iceberg.{staging_query_output_table_name}")
        
        #TODO: Remove the statement below once we migrate staging query from print to log stmts
        self._print_with_timestamp("See Cluster Details > Driver Logs > Standard Output for additional details.")
        return result_df