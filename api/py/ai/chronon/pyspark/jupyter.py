#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

"""
Jupyter Notebook implementation for Chronon PySpark interface.

This module provides Jupyter-specific implementations for executing Chronon
features in Jupyter notebook environments (including JupyterLab, JupyterHub,
and classic Jupyter notebooks).

Example usage:
    from pyspark.sql import SparkSession
    from ai.chronon.pyspark.jupyter import JupyterGroupBy, JupyterJoin
    from ai.chronon.api.ttypes import GroupBy, Join

    # Create your GroupBy or Join object
    my_group_by = GroupBy(...)

    # Execute in Jupyter
    executable = JupyterGroupBy(my_group_by, spark)
    result_df = executable.run(start_date='20250101', end_date='20250107')
"""

from __future__ import annotations

import getpass
import io
import logging
from typing import Any, Optional

from pyspark.sql import SparkSession
from typing_extensions import override

from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.pyspark.constants import (
    JUPYTER_JVM_LOG_FILE,
    JUPYTER_OUTPUT_NAMESPACE,
    JUPYTER_ROOT_DIR_FOR_IMPORTED_FEATURES,
)
from ai.chronon.pyspark.executables import (
    GroupByExecutable,
    JoinExecutable,
    PlatformInterface,
)


class JupyterLogCapture:
    """
    A context manager for capturing JVM/Spark logs in Jupyter environments.

    This class captures log output by redirecting stdout and using a custom
    logging handler. It's designed to work in notebook environments where
    logs need to be displayed inline.
    """

    def __init__(self, job_name: str):
        """
        Initialize the log capture.

        Args:
            job_name: The name of the job for log headers
        """
        self.job_name = job_name
        self.log_buffer = io.StringIO()
        self.handler: Optional[logging.StreamHandler] = None
        self._original_stdout: Optional[Any] = None

    def start(self) -> "JupyterLogCapture":
        """
        Start capturing logs.

        Returns:
            Self for chaining
        """
        # Create a handler that writes to our StringIO buffer
        self.handler = logging.StreamHandler(self.log_buffer)
        self.handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.handler.setFormatter(formatter)

        # Add handler to py4j and pyspark loggers to capture JVM communication
        for logger_name in ['py4j', 'pyspark', 'ai.chronon']:
            logger = logging.getLogger(logger_name)
            logger.addHandler(self.handler)

        return self

    def stop(self) -> str:
        """
        Stop capturing logs and return the captured content.

        Returns:
            The captured log content
        """
        # Remove our handler from loggers
        if self.handler:
            for logger_name in ['py4j', 'pyspark', 'ai.chronon']:
                logger = logging.getLogger(logger_name)
                logger.removeHandler(self.handler)

        # Get captured content
        content = self.log_buffer.getvalue()
        self.log_buffer.close()

        return content


class JupyterPlatform(PlatformInterface):
    """
    Jupyter-specific implementation of the platform interface.

    This platform works with standard Jupyter notebooks, JupyterLab,
    JupyterHub, and other Jupyter-compatible environments.
    """

    def __init__(
        self,
        spark: SparkSession,
        log_file: Optional[str] = None
    ):
        """
        Initialize Jupyter-specific components.

        Args:
            spark: The SparkSession to use
            log_file: Optional path to a JVM log file for log capture.
                     If not provided, uses in-memory log capture.
        """
        self.log_file = log_file or JUPYTER_JVM_LOG_FILE
        self._use_file_logging = log_file is not None
        super().__init__(spark)

    @override
    def register_udfs(self) -> None:
        """
        Register UDFs for Jupyter.

        Override this method if you need to register custom UDFs
        for your Jupyter environment.
        """
        pass

    @override
    def get_executable_join_cls(self) -> type[JoinExecutable]:
        """Get the Jupyter-specific join executable class."""
        return JupyterJoin

    @override
    def start_log_capture(self, job_name: str) -> Any:
        """
        Start capturing logs in Jupyter.

        This method supports two modes:
        1. File-based logging: If a log file path is configured, tracks
           file position (similar to Databricks)
        2. In-memory logging: Uses Python logging handlers to capture
           py4j and pyspark logs

        Args:
            job_name: The name of the job for log headers

        Returns:
            A log capture token (file position tuple or JupyterLogCapture)
        """
        if self._use_file_logging:
            try:
                import os
                return (os.path.getsize(self.log_file), job_name)
            except (OSError, IOError):
                # Fall back to in-memory logging if file doesn't exist
                pass

        # Use in-memory log capture
        capture = JupyterLogCapture(job_name)
        capture.start()
        return capture

    @override
    def end_log_capture(self, capture_token: Any) -> None:
        """
        End log capture and print logs in Jupyter.

        Args:
            capture_token: The token returned by start_log_capture
        """
        if capture_token is None:
            return

        # Handle file-based logging
        if isinstance(capture_token, tuple) and len(capture_token) == 2:
            start_position, job_name = capture_token
            try:
                print("\n\n", "*" * 10, f" BEGIN LOGS FOR {job_name} ", "*" * 10)
                with open(self.log_file, "r") as file_handler:
                    _ = file_handler.seek(start_position)
                    print(file_handler.read())
                print("*" * 10, f" END LOGS FOR {job_name} ", "*" * 10, "\n\n")
            except (OSError, IOError) as e:
                print(f"Warning: Could not read log file: {e}")
            return

        # Handle in-memory log capture
        if isinstance(capture_token, JupyterLogCapture):
            content = capture_token.stop()
            job_name = capture_token.job_name

            print("\n\n", "*" * 10, f" BEGIN LOGS FOR {job_name} ", "*" * 10)
            if content.strip():
                print(content)
            else:
                print("(No logs captured - JVM logs may not be routed to Python)")
            print("*" * 10, f" END LOGS FOR {job_name} ", "*" * 10, "\n\n")

    def get_jupyter_user(self) -> str:
        """
        Get the current Jupyter user.

        This uses getpass.getuser() which works across different
        Jupyter deployment methods (local, JupyterHub, etc.)

        Returns:
            The username of the current user
        """
        try:
            return getpass.getuser().lower().replace(".", "_").replace("-", "_")
        except Exception:
            return "jupyter_user"


class JupyterGroupBy(GroupByExecutable):
    """
    Class for executing GroupBy objects in Jupyter notebooks.

    Example:
        from ai.chronon.pyspark.jupyter import JupyterGroupBy

        executable = JupyterGroupBy(my_group_by, spark)
        df = executable.run(start_date='20250101', end_date='20250107')
    """

    def __init__(
        self,
        group_by: GroupBy,
        spark_session: SparkSession,
        name_prefix: Optional[str] = None,
        output_namespace: Optional[str] = None,
        use_username_prefix: bool = True
    ):
        """
        Initialize a GroupBy executor for Jupyter.

        Args:
            group_by: The GroupBy object to execute
            spark_session: The SparkSession to use
            name_prefix: Optional custom prefix for the object name.
                        If not provided and use_username_prefix is True,
                        uses the current username.
            output_namespace: Optional namespace override for output tables
            use_username_prefix: Whether to prefix object names with username
                                (default: True)
        """
        super().__init__(group_by, spark_session)

        # Determine name prefix
        effective_name_prefix: Optional[str] = None
        if name_prefix:
            effective_name_prefix = name_prefix
        elif use_username_prefix:
            jupyter_platform = self.platform
            if isinstance(jupyter_platform, JupyterPlatform):
                effective_name_prefix = jupyter_platform.get_jupyter_user()

        self.obj: GroupBy = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=JUPYTER_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=effective_name_prefix,
            output_namespace=output_namespace or JUPYTER_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Jupyter platform interface
        """
        return JupyterPlatform(self.spark)


class JupyterJoin(JoinExecutable):
    """
    Class for executing Join objects in Jupyter notebooks.

    Example:
        from ai.chronon.pyspark.jupyter import JupyterJoin

        executable = JupyterJoin(my_join, spark)
        df = executable.run(start_date='20250101', end_date='20250107')
    """

    def __init__(
        self,
        join: Join,
        spark_session: SparkSession,
        name_prefix: Optional[str] = None,
        output_namespace: Optional[str] = None,
        use_username_prefix: bool = True
    ):
        """
        Initialize a Join executor for Jupyter.

        Args:
            join: The Join object to execute
            spark_session: The SparkSession to use
            name_prefix: Optional custom prefix for the object name.
                        If not provided and use_username_prefix is True,
                        uses the current username.
            output_namespace: Optional namespace override for output tables
            use_username_prefix: Whether to prefix object names with username
                                (default: True)
        """
        super().__init__(join, spark_session)

        # Determine name prefix
        effective_name_prefix: Optional[str] = None
        if name_prefix:
            effective_name_prefix = name_prefix
        elif use_username_prefix:
            jupyter_platform = self.platform
            if isinstance(jupyter_platform, JupyterPlatform):
                effective_name_prefix = jupyter_platform.get_jupyter_user()

        self.obj: Join = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=JUPYTER_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=effective_name_prefix,
            output_namespace=output_namespace or JUPYTER_OUTPUT_NAMESPACE
        )

    @override
    def get_platform(self) -> PlatformInterface:
        """
        Get the platform interface.

        Returns:
            The Jupyter platform interface
        """
        return JupyterPlatform(self.spark)
