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
Local testing platform for Chronon PySpark interface.

Provides a simple BYOD (Bring Your Own Data) model for running GroupBy, Join,
and StagingQuery computations locally with in-memory data. Users provide
DataFrames mapped to table names, and the platform handles everything needed
to execute Chronon's JVM computation engine locally.

Example usage:
    from ai.chronon.pyspark.local import (
        run_local_group_by,
        run_local_join,
        run_local_staging_query,
        reset_local_session,
    )

    # GroupBy
    result_df = run_local_group_by(
        group_by=my_gb,
        tables={"events.payments": events_df},
        start_date="20220101",
        end_date="20220107",
    )

    # Join
    result_df = run_local_join(
        join=my_join,
        tables={"events.payments": events_df, "dim.users": users_df},
        start_date="20220101",
        end_date="20220107",
    )

    # StagingQuery
    result_df = run_local_staging_query(
        staging_query=my_sq,
        tables={"raw.events": events_df},
        end_date="20220107",
    )

    # Reset if needed (clears all tables, fresh session)
    reset_local_session()
"""

from __future__ import annotations

import gc
import importlib
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional, Union

from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from typing_extensions import override

from ai.chronon.api.ttypes import GroupBy, Join, StagingQuery
from ai.chronon.pyspark.constants import (
    LOCAL_NAME_PREFIX,
    LOCAL_OUTPUT_NAMESPACE,
    LOCAL_ROOT_DIR_FOR_IMPORTED_FEATURES,
)
from ai.chronon.pyspark.executables import (
    GroupByExecutable,
    JoinExecutable,
    PlatformInterface,
    StagingQueryExecutable,
)
from ai.chronon.pyspark.jupyter import JupyterLogCapture
from ai.chronon.repo.extract_objects import import_module_set_name


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_local_spark_session: Optional[SparkSession] = None
_local_warehouse_dir: Optional[str] = None


def _get_or_create_session() -> SparkSession:
    """Get or lazily create the singleton local SparkSession."""
    global _local_spark_session
    if _local_spark_session is None or _local_spark_session._sc._jsc is None:
        _local_spark_session = create_local_spark_session()
    return _local_spark_session


# ---------------------------------------------------------------------------
# Auto-naming
# ---------------------------------------------------------------------------

# Map from thrift type to the conventional module prefix for that type.
_TYPE_TO_PREFIXES: dict[type, list[str]] = {
    GroupBy: ["group_bys", "groupbys", "group_by"],
    Join: ["joins", "join"],
    StagingQuery: ["staging_queries", "staging_query"],
}


def _ensure_name(obj: Union[GroupBy, Join, StagingQuery]) -> None:
    """Set metaData.name on *obj* if it is not already set.

    Uses the garbage collector to find the Python module where *obj* was
    defined, then calls ``import_module_set_name`` — the same mechanism
    Chronon's compile pipeline uses — to derive name and team from the
    module path.
    """
    if obj.metaData and obj.metaData.name:
        return

    obj_type = type(obj)
    prefixes = _TYPE_TO_PREFIXES.get(obj_type, [])

    gc.collect()
    for ref in gc.get_referrers(obj):
        if not isinstance(ref, dict) or "__name__" not in ref:
            continue
        mod_name = ref["__name__"]
        if any(mod_name.startswith(p) for p in prefixes):
            module = importlib.import_module(mod_name)
            import_module_set_name(module, obj_type)
            return

    # Also set names on nested GroupBys inside JoinParts
    if obj_type == Join and obj.joinParts:
        for jp in obj.joinParts:
            if jp.groupBy:
                _ensure_name(jp.groupBy)


# ---------------------------------------------------------------------------
# Convenience Functions (Primary API)
# ---------------------------------------------------------------------------


def _enable_historical_backfill(obj: Union[GroupBy, Join, StagingQuery]) -> Optional[bool]:
    """Enable historicalBackfill for local testing if not explicitly set.

    When historicalBackfill is None, the JVM treats it as false, which means
    only the latest partition is backfilled. For local testing we always want
    full backfill over the requested date range, so we default to True.

    Returns the original value so the caller can restore it after execution.
    """
    if obj.metaData and obj.metaData.historicalBackfill is None:
        original = obj.metaData.historicalBackfill
        obj.metaData.historicalBackfill = True
        return original
    return obj.metaData.historicalBackfill if obj.metaData else None


def _restore_historical_backfill(
    obj: Union[GroupBy, Join, StagingQuery], original: Optional[bool]
) -> None:
    """Restore historicalBackfill to its original value."""
    if obj.metaData:
        obj.metaData.historicalBackfill = original


def run_local_group_by(
    group_by: GroupBy,
    tables: dict[str, DataFrame],
    start_date: str | None = None,
    end_date: str | None = None,
    step_days: int = 30,
) -> DataFrame:
    """Run a GroupBy computation locally with provided DataFrames.

    Args:
        group_by: The GroupBy definition to execute.
        tables: Mapping of table names to DataFrames (e.g. {"events.payments": df}).
        start_date: Start date for computation (format: YYYYMMDD).
        end_date: End date for computation (format: YYYYMMDD).
        step_days: Number of days to process in each step.

    Returns:
        DataFrame with the GroupBy results.
    """
    _ensure_name(group_by)
    original = _enable_historical_backfill(group_by)
    spark = _get_or_create_session()
    register_tables(spark, tables)
    try:
        executable = LocalGroupBy(group_by, spark)
        return executable.run(start_date=start_date, end_date=end_date, step_days=step_days)
    finally:
        _restore_historical_backfill(group_by, original)


def run_local_join(
    join: Join,
    tables: dict[str, DataFrame],
    start_date: str | None = None,
    end_date: str | None = None,
    step_days: int = 30,
) -> DataFrame:
    """Run a Join computation locally with provided DataFrames.

    Args:
        join: The Join definition to execute.
        tables: Mapping of table names to DataFrames.
        start_date: Start date for computation (format: YYYYMMDD).
        end_date: End date for computation (format: YYYYMMDD).
        step_days: Number of days to process in each step.

    Returns:
        DataFrame with the Join results.
    """
    _ensure_name(join)
    # Enable historicalBackfill on the Join and all nested GroupBys
    originals = [(join, _enable_historical_backfill(join))]
    if join.joinParts:
        for jp in join.joinParts:
            if jp.groupBy:
                originals.append((jp.groupBy, _enable_historical_backfill(jp.groupBy)))
    spark = _get_or_create_session()
    register_tables(spark, tables)
    try:
        executable = LocalJoin(join, spark)
        return executable.run(start_date=start_date, end_date=end_date, step_days=step_days)
    finally:
        for obj, orig in originals:
            _restore_historical_backfill(obj, orig)


def run_local_staging_query(
    staging_query: StagingQuery,
    tables: dict[str, DataFrame],
    end_date: str | None = None,
    step_days: int | None = None,
) -> DataFrame:
    """Run a StagingQuery computation locally with provided DataFrames.

    Args:
        staging_query: The StagingQuery definition to execute.
        tables: Mapping of table names to DataFrames.
        end_date: End date for computation (format: YYYYMMDD).
        step_days: Number of days to process in each step.

    Returns:
        DataFrame with the StagingQuery results.
    """
    _ensure_name(staging_query)
    original = _enable_historical_backfill(staging_query)
    spark = _get_or_create_session()
    register_tables(spark, tables)
    try:
        executable = LocalStagingQuery(staging_query, spark)
        return executable.run(end_date=end_date, step_days=step_days)
    finally:
        _restore_historical_backfill(staging_query, original)


def reset_local_session() -> None:
    """Stop the singleton SparkSession, clear the warehouse, and reset state."""
    global _local_spark_session, _local_warehouse_dir
    if _local_spark_session is not None:
        try:
            _local_spark_session.stop()
        except Exception:
            pass
        _local_spark_session = None
    if _local_warehouse_dir is not None:
        shutil.rmtree(_local_warehouse_dir, ignore_errors=True)
        _local_warehouse_dir = None


# ---------------------------------------------------------------------------
# Session Creation
# ---------------------------------------------------------------------------


def _find_chronon_jars(extra_jars: list[str] | None = None) -> str:
    """Discover Chronon JARs from the repo, env var, or provided paths.

    JAR discovery order:
    1. CHRONON_SPARK_JAR environment variable (single deploy JAR path, used by bazel)
    2. SBT build output directories (target/scala-2.12/)
    3. extra_jars parameter

    Args:
        extra_jars: Optional list of extra JAR paths to include.

    Returns:
        Colon-separated classpath string.
    """
    import os

    jars: list[str] = []

    # Check for bazel-provided JAR via environment variable
    env_jar = os.environ.get("CHRONON_SPARK_JAR")
    if env_jar and Path(env_jar).exists():
        jars.append(env_jar)
    else:
        # Try to find JARs from the SBT repo structure
        # Walk up from this file to find the repo root
        current = Path(__file__).resolve()
        # Go up: local.py -> pyspark -> chronon -> ai -> py -> api -> <repo_root>
        chronon_root = current.parents[5]
        spark_target = chronon_root / "spark" / "target" / "scala-2.12"

        # Prefer the assembly (fat) JAR — it bundles all transitive deps
        assembly_candidates = [
            j for j in spark_target.glob("spark_uber_2.12-*-assembly.jar")
        ] if spark_target.exists() else []

        if assembly_candidates:
            jars.append(str(assembly_candidates[0]))
        else:
            # Fall back to individual thin JARs from publishLocal
            jar_configs = [
                (chronon_root / "api" / "target" / "scala-2.12", "api"),
                (spark_target, "spark_uber"),
                (chronon_root / "aggregator" / "target" / "scala-2.12", "aggregator"),
                (chronon_root / "online" / "target" / "scala-2.12", "online"),
            ]

            for target_dir, artifact_name in jar_configs:
                if not target_dir.exists():
                    continue
                pattern = f"{artifact_name}_2.12-*.jar"
                candidates = [
                    j for j in target_dir.glob(pattern)
                    if not j.name.endswith(("-sources.jar", "-javadoc.jar", "-assembly.jar"))
                ]
                if candidates:
                    jars.append(str(candidates[0]))

    if extra_jars:
        jars.extend(extra_jars)

    if not jars:
        raise RuntimeError(
            "No Chronon JARs found. Set CHRONON_SPARK_JAR env var, "
            "run from within the Chronon repo "
            "(after `sbt '++ 2.12.12; spark_uber/assembly'`), or pass extra_jars."
        )

    return ":".join(jars)


def create_local_spark_session(
    extra_jars: list[str] | None = None,
) -> SparkSession:
    """Create a local SparkSession configured for Chronon testing.

    Uses an in-memory Derby metastore and a temp warehouse directory so that
    DataFrames can be saved as real Hive tables accessible to the JVM.

    Args:
        extra_jars: Optional list of extra JAR paths to include on the classpath.

    Returns:
        A configured SparkSession.
    """
    global _local_warehouse_dir

    classpath = _find_chronon_jars(extra_jars)

    # Create a unique temp warehouse directory
    _local_warehouse_dir = tempfile.mkdtemp(prefix="chronon_local_")
    derby_name = f"metastore_db_{uuid.uuid4().hex[:8]}"

    # Stop any existing session so our classpath takes effect
    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
        SparkSession._instantiatedSession = None  # type: ignore[attr-defined]

    import os
    import sys

    python_exec = os.path.realpath(sys.executable)
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    spark_conf = (
        SparkConf()
        .setMaster("local[2]")
        .setAppName("chronon-local-testing")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.extraClassPath", classpath)
        .set("spark.executor.extraClassPath", classpath)
        .set("spark.driver.host", "127.0.0.1")
        # Ensure driver and worker use the same Python interpreter
        .set("spark.pyspark.python", python_exec)
        .set("spark.pyspark.driver.python", python_exec)
        # Low parallelism for local testing
        .set("spark.sql.shuffle.partitions", "2")
        .set("spark.default.parallelism", "2")
        # In-memory Derby metastore
        .set(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:memory:{derby_name};create=true",
        )
        .set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        # Temp warehouse
        .set("spark.sql.warehouse.dir", _local_warehouse_dir)
        # Chronon partition settings
        .set("spark.chronon.partition.column", "ds")
        .set("spark.chronon.partition.format", "yyyyMMdd")
    )

    spark = (
        SparkSession.builder
        .config(conf=spark_conf)
        .enableHiveSupport()
        .getOrCreate()
    )

    # Validate that Chronon JARs are accessible
    try:
        spark._jvm.ai.chronon.spark.PySparkUtils
    except Exception as e:
        spark.stop()
        raise RuntimeError(
            f"Chronon JARs not loaded correctly. Classpath: {classpath}. Error: {e}"
        )

    return spark


# ---------------------------------------------------------------------------
# Table Registration
# ---------------------------------------------------------------------------


def register_tables(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    partition_column: str = "ds",
) -> None:
    """Register DataFrames as Hive tables so the JVM can query them.

    Args:
        spark: The SparkSession to use.
        tables: Mapping of fully-qualified table names (e.g. "db.table") to DataFrames.
        partition_column: Name of the partition column (default: "ds").

    Raises:
        ValueError: If a DataFrame is missing the partition column.
    """
    # Ensure the output namespace database exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {LOCAL_OUTPUT_NAMESPACE}")

    for full_name, df in tables.items():
        # Validate partition column
        if partition_column not in df.columns:
            raise ValueError(
                f"DataFrame for table '{full_name}' is missing the partition "
                f"column '{partition_column}'. Columns: {df.columns}"
            )

        # Parse database and table name
        parts = full_name.split(".", 1)
        if len(parts) == 2:
            database, table = parts
        else:
            database = "default"
            table = parts[0]

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        df.write.partitionBy(partition_column).mode("overwrite").saveAsTable(
            f"{database}.{table}"
        )


# ---------------------------------------------------------------------------
# Platform Classes
# ---------------------------------------------------------------------------


class LocalTestingPlatform(PlatformInterface):
    """Local testing implementation of the platform interface."""

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    @override
    def register_udfs(self) -> None:
        pass

    @override
    def get_executable_join_cls(self) -> type[JoinExecutable]:
        return LocalJoin

    @override
    def start_log_capture(self, job_name: str) -> Any:
        capture = JupyterLogCapture(job_name)
        capture.start()
        return capture

    @override
    def end_log_capture(self, capture_token: Any) -> None:
        if isinstance(capture_token, JupyterLogCapture):
            content = capture_token.stop()
            job_name = capture_token.job_name
            print("\n", "*" * 10, f" BEGIN LOGS FOR {job_name} ", "*" * 10)
            if content.strip():
                print(content)
            print("*" * 10, f" END LOGS FOR {job_name} ", "*" * 10, "\n")

    @override
    def get_table_utils(self) -> Any:
        # TableUtils was moved to the catalog subpackage
        return self.jvm.ai.chronon.spark.catalog.TableUtils(
            self.java_spark_session
        )

    @override
    def drop_table_if_exists(self, table_name: str) -> None:
        # Ensure the database exists before trying to drop a table in it
        parts = table_name.split(".", 1)
        if len(parts) == 2:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {parts[0]}")
        super().drop_table_if_exists(table_name)


class LocalGroupBy(GroupByExecutable):
    """Execute a GroupBy locally with in-memory data."""

    def __init__(
        self,
        group_by: GroupBy,
        spark_session: SparkSession,
        name_prefix: str | None = None,
        output_namespace: str | None = None,
    ):
        super().__init__(group_by, spark_session)
        self.obj: GroupBy = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=LOCAL_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=name_prefix or LOCAL_NAME_PREFIX,
            output_namespace=output_namespace or LOCAL_OUTPUT_NAMESPACE,
        )

    @override
    def get_platform(self) -> PlatformInterface:
        return LocalTestingPlatform(self.spark)


class LocalJoin(JoinExecutable):
    """Execute a Join locally with in-memory data."""

    def __init__(
        self,
        join: Join,
        spark_session: SparkSession,
        name_prefix: str | None = None,
        output_namespace: str | None = None,
    ):
        # JVM requires team to be set
        if not join.metaData.team:
            join.metaData.team = "local"
        super().__init__(join, spark_session)
        self.obj: Join = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=LOCAL_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=name_prefix or LOCAL_NAME_PREFIX,
            output_namespace=output_namespace or LOCAL_OUTPUT_NAMESPACE,
        )

    @override
    def get_platform(self) -> PlatformInterface:
        return LocalTestingPlatform(self.spark)


class LocalStagingQuery(StagingQueryExecutable):
    """Execute a StagingQuery locally with in-memory data."""

    def __init__(
        self,
        staging_query: StagingQuery,
        spark_session: SparkSession,
        name_prefix: str | None = None,
        output_namespace: str | None = None,
    ):
        super().__init__(staging_query, spark_session)
        self.obj: StagingQuery = self.platform.set_metadata(
            obj=self.obj,
            mod_prefix=LOCAL_ROOT_DIR_FOR_IMPORTED_FEATURES,
            name_prefix=name_prefix or LOCAL_NAME_PREFIX,
            output_namespace=output_namespace or LOCAL_OUTPUT_NAMESPACE,
        )

    @override
    def get_platform(self) -> PlatformInterface:
        return LocalTestingPlatform(self.spark)
