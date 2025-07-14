from __future__ import annotations

import os
from typing import cast

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from typing_extensions import override

from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.pyspark.constants import (
    DATABRICKS_JVM_LOG_FILE,
    DATABRICKS_OUTPUT_NAMESPACE,
    DATABRICKS_ROOT_DIR_FOR_IMPORTED_FEATURES,
)
from ai.chronon.pyspark.executables import (
    GroupByExecutable,
    JoinExecutable,
    PlatformInterface,
)


class DatabricksPlatform(PlatformInterface):
    """
    Databricks-specific implementation of the platform interface.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize Databricks-specific components.

        Args:
            spark: The SparkSession to use
        """
        super().__init__(spark)
        self.dbutils: DBUtils = DBUtils(self.spark)
        self.register_udfs()

    @override
    def register_udfs(self) -> None:
        """Register UDFs for Databricks."""
        pass

    @override
    def get_executable_join_cls(self) -> type[JoinExecutable]:
        """Get the Databricks-specific join executable class."""
        return DatabricksJoin

    @override
    def start_log_capture(self, job_name: str) -> tuple[int, str]:
        """
        Start capturing logs in Databricks.

        Args:
            job_name: The name of the job for log headers

        Returns:
            A tuple of (start_position, job_name)
        """
        return (os.path.getsize(DATABRICKS_JVM_LOG_FILE), job_name)

    @override
    def end_log_capture(self, capture_token: tuple[int, str]) -> None:
        """
        End log capture and print logs in Databricks.

        Args:
            capture_token: The token returned by start_log_capture
        """
        start_position, job_name = capture_token

        print("\n\n", "*" * 10, f" BEGIN LOGS FOR {job_name} ", "*" * 10)
        with open(DATABRICKS_JVM_LOG_FILE, "r") as file_handler:
            _ = file_handler.seek(start_position)
            print(file_handler.read())
        print("*" * 10, f" END LOGS FOR {job_name} ", "*" * 10, "\n\n")

    def get_databricks_user(self) -> str:
        """
        Get the current Databricks user.

        Returns:
            The username of the current Databricks user
        """
        user_email = self.dbutils.notebook.entry_point.getDbutils().notebook(
        ).getContext().userName().get()
        return user_email.split('@')[0].lower()


class DatabricksGroupBy(GroupByExecutable):
    """Class for executing GroupBy objects in Databricks."""

    def __init__(self, group_by: GroupBy, spark_session: SparkSession):
        """
        Initialize a GroupBy executor for Databricks.

        Args:
            group_by: The GroupBy object to execute
            spark_session: The SparkSession to use
        """
        super().__init__(group_by, spark_session)

        self.obj: GroupBy = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=DATABRICKS_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=cast(DatabricksPlatform, self.platform).get_databricks_user(),
            output_namespace=DATABRICKS_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Databricks platform interface
        """
        return DatabricksPlatform(self.spark)


class DatabricksJoin(JoinExecutable):
    """Class for executing Join objects in Databricks."""

    def __init__(self, join: Join, spark_session: SparkSession):
        """
        Initialize a Join executor for Databricks.

        Args:
            join: The Join object to execute
            spark_session: The SparkSession to use
        """
        super().__init__(join, spark_session)

        self.obj: Join = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=DATABRICKS_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=cast(DatabricksPlatform, self.platform).get_databricks_user(),
            output_namespace=DATABRICKS_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Databricks platform interface
        """
        return DatabricksPlatform(self.spark)
