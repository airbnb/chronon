from __future__ import annotations

try:
    from pyspark.dbutils import DBUtils
except ImportError:
    class DBUtils:
        def __init__(self, spark):
            print("Mock DBUtils: Notebooks-specific features will not work.")
        def fs(self):
            return self
        def ls(self, path):
            print(f"Mock ls called on path: {path}")
            return []
        def mkdirs(self, path):
            print(f"Mock mkdirs called on path: {path}")
        def put(self, path, content, overwrite):
            print(f"Mock put called on path: {path} with content and overwrite={overwrite}")


from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from typing_extensions import override

from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.pyspark.constants import (
    # NOTEBOOKS_JVM_LOG_FILE, --> Commented by ONUR
    NOTEBOOKS_OUTPUT_NAMESPACE,
    NOTEBOOKS_ROOT_DIR_FOR_IMPORTED_FEATURES,
)
from ai.chronon.pyspark.executables import (
    GroupByExecutable,
    JoinExecutable,
    PlatformInterface,
)


class NotebooksPlatform(PlatformInterface):
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
        self.dbutils: DBUtils = DBUtils(self.spark)
        self.register_udfs()

    @override
    def register_udfs(self) -> None:
        """Register UDFs for Notebooks."""
        pass

    @override
    def get_executable_join_cls(self) -> type[JoinExecutable]:
        """Get the Notebooks-specific join executable class."""
        return NotebooksJoin

    @override
    def start_log_capture(self, job_name: str) -> tuple[int, str]:
        """
        Start capturing logs in Notebooks.

        Args:
            job_name: The name of the job for log headers

        Returns:
            A tuple of (start_position, job_name)
        """
        # return (os.path.getsize(NOTEBOOKS_JVM_LOG_FILE), job_name) --> Commented by odincol
        return (0, job_name)                                        # --> Refactored by odincol

    @override
    def end_log_capture(self, capture_token: tuple[int, str]) -> None:
        """
        End log capture and print logs in Notebooks.

        Args:
            capture_token: The token returned by start_log_capture
        """
        start_position, job_name = capture_token

        print("\n\n", "*" * 10, f" BEGIN LOGS FOR {job_name} ", "*" * 10)
        # with open(NOTEBOOKS_JVM_LOG_FILE, "r") as file_handler:   --> Commented by odincol
        #     _ = file_handler.seek(start_position)
        #     print(file_handler.read())
        print("*" * 10, f" END LOGS FOR {job_name} ", "*" * 10, "\n\n")

    def get_notebooks_user(self) -> str:
        """
        Get the current Notebooks user.

        Returns:
            The username of the current Notebooks user
        """
        #user_email = self.dbutils.notebook.entry_point.getDbutils().notebook( --> Commented by odincol
        #).getContext().userName().get()
        return "" #user_email.split('@')[0].lower()                          # --> Refactored by odincol


class NotebooksGroupBy(GroupByExecutable):
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
            mod_prefix=NOTEBOOKS_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=cast(NotebooksPlatform, self.platform).get_notebooks_user(),
            output_namespace=NOTEBOOKS_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Notebooks platform interface
        """
        return NotebooksPlatform(self.spark)


class NotebooksJoin(JoinExecutable):
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
            mod_prefix=NOTEBOOKS_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=cast(NotebooksPlatform, self.platform).get_notebooks_user(),
            output_namespace=NOTEBOOKS_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Notebooks platform interface
        """
        return NotebooksPlatform(self.spark)
