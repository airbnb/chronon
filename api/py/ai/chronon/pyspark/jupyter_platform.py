from __future__ import annotations

from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.pyspark.constants import (
    JUPYTER_OUTPUT_NAMESPACE,
    JUPYTER_ROOT_DIR_FOR_IMPORTED_FEATURES,
)
from ai.chronon.pyspark.executables import (
    GroupByExecutable,
    JoinExecutable,
    PlatformInterface,
)
from pyspark.sql import SparkSession
from typing_extensions import override


class JupyterPlatform(PlatformInterface):
    """
    Notebooks-specific implementation of the platform interface.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize Notebooks-specific components.

        Args:
            spark: The SparkSession to use
        """
        super().__init__(spark)
        self.register_udfs()

    @override
    def register_udfs(self) -> None:
        """Register UDFs for Notebooks."""
        pass

    @override
    def get_executable_join_cls(self) -> type[JoinExecutable]:
        """Get the Notebooks-specific join executable class."""
        return JupyterJoin

    @override
    def start_log_capture(self, job_name: str) -> tuple[int, str]:
        """
        Start capturing logs in Notebooks.

        Args:
            job_name: The name of the job for log headers

        Returns:
            A tuple of (start_position, job_name)
        """
        pass

    @override
    def end_log_capture(self, capture_token: tuple[int, str]) -> None:
        """
        End log capture and print logs in Notebooks.

        Args:
            capture_token: The token returned by start_log_capture
        """
        pass


class JupyterGroupBy(GroupByExecutable):
    """Class for executing GroupBy objects in Notebooks."""

    def __init__(self, group_by: GroupBy, spark_session: SparkSession):
        """
        Initialize a GroupBy executor for Notebooks.

        Args:
            group_by: The GroupBy object to execute
            spark_session: The SparkSession to use
        """
        super().__init__(group_by, spark_session)

        self.obj: GroupBy = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=JUPYTER_ROOT_DIR_FOR_IMPORTED_FEATURES,
            output_namespace=JUPYTER_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Notebooks platform interface
        """
        return JupyterPlatform(self.spark)


class JupyterJoin(JoinExecutable):
    """Class for executing Join objects in Notebooks."""

    def __init__(self, join: Join, spark_session: SparkSession):
        """
        Initialize a Join executor for Notebooks.

        Args:
            join: The Join object to execute
            spark_session: The SparkSession to use
        """
        super().__init__(join, spark_session)

        self.obj: Join = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=JUPYTER_ROOT_DIR_FOR_IMPORTED_FEATURES,
            output_namespace=JUPYTER_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Notebooks platform interface
        """
        return JupyterPlatform(self.spark)
