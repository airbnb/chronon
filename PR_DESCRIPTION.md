# CHIP-10: Chronon PySpark Notebooks Interface

## Summary

This PR contributes Stripe's PySpark notebooks integration for Chronon to the open-source repository. It enables ML engineers to define, execute, and analyze Chronon features (GroupBy, Join, and StagingQuery) entirely within notebook environments like Databricks and Jupyter.

The implementation uses a Python-JVM bridge (Py4J) to communicate with Chronon's Scala computation engine, allowing feature development without leaving the notebook environment.

## Key Features

### 1. Platform-Agnostic Executable Framework
- **`PySparkExecutable`** - Abstract base class providing common Python-JVM bridge functionality
- **`GroupByExecutable`** / **`JoinExecutable`** / **`StagingQueryExecutable`** - Type-specific interfaces with `run()` and `analyze()` methods
- **`PlatformInterface`** - Abstract interface for platform-specific behavior (logging, UDF registration, etc.)

### 2. Databricks Integration (`databricks.py`)
- `DatabricksPlatform` - Platform implementation with DBUtils integration and JVM log capture
- `DatabricksGroupBy` / `DatabricksJoin` / `DatabricksStagingQuery` - Ready-to-use executables
- Automatic username prefixing for table isolation

### 3. Jupyter Integration (`jupyter.py`) - NEW
- `JupyterPlatform` - Platform implementation supporting both file-based and in-memory log capture
- `JupyterGroupBy` / `JupyterJoin` / `JupyterStagingQuery` - Executables for JupyterLab, JupyterHub, and classic notebooks
- Configurable `name_prefix`, `output_namespace`, and `use_username_prefix` options
- `JupyterLogCapture` - Helper class for capturing py4j/pyspark logs in notebooks

### 4. StagingQuery Support - NEW
- Full implementation of `StagingQueryExecutable` for materializing intermediate SQL transformations
- Scala JVM bridge methods: `parseStagingQuery()`, `runStagingQuery()`, `getBooleanOptional()`
- Support for date templates: `{{ start_date }}`, `{{ end_date }}`, `{{ latest_date }}`, `{{ max_date(table=...) }}`
- Parameters: `step_days`, `enable_auto_expand`, `override_start_partition`, `skip_first_hole`

### 5. PySpark Integration Tests - NEW
- **`test_pyspark.py`** - End-to-end integration tests verifying Python-JVM communication
- **`test_helpers.py`** - Utility functions for creating mock GroupBys and running aggregations through JVM
- Tests verify the complete flow: Python thrift → JSON → Scala parsing → Spark execution → DataFrame return

### 6. Scala Unit Tests (`PySparkUtilsTest.scala`)
- 18 unit tests covering all `PySparkUtils` methods
- Tests for parsing (GroupBy, Join, StagingQuery), Optional helpers, and resolution utilities

## Architecture

```
Python (Notebook)  <--Py4J-->  JVM (Spark/Chronon)
     |                              |
  GroupBy/Join                 PySparkUtils.scala
  (Thrift Obj)    --> JSON -->  parseGroupBy/Join
     |                              |
  .run()          <-- DF  <--   runGroupBy/Join
```

## CI Changes

To enable PySpark integration tests in CI, we made the following changes to `.circleci/config.yml`:

1. **JAR Building** - Added `sbt publishLocal` step to Python test job to compile Chronon JARs
2. **SBT Caching** - Added cache restore/save for `~/.sbt`, `~/.ivy2/cache`, and `~/.cache/coursier` to speed up builds
3. **JAR Discovery** - Dynamic JAR path discovery using glob patterns to handle versioned artifacts

The Python tests now:
- Build the Scala JARs before running
- Configure SparkSession with correct classpath
- Verify JVM classes are loadable before running tests

## Files Changed

### Python (`api/py/ai/chronon/pyspark/`)
| File | Description |
|------|-------------|
| `executables.py` | Base classes: `PySparkExecutable`, `GroupByExecutable`, `JoinExecutable`, `StagingQueryExecutable`, `PlatformInterface` |
| `databricks.py` | Databricks-specific: `DatabricksPlatform`, `DatabricksGroupBy`, `DatabricksJoin`, `DatabricksStagingQuery` |
| `jupyter.py` | **NEW** - Jupyter-specific: `JupyterPlatform`, `JupyterGroupBy`, `JupyterJoin`, `JupyterStagingQuery` |
| `constants.py` | Platform-specific constants (log paths, namespaces) |
| `README.md` | Comprehensive documentation with architecture, examples, and setup guide |

### Python Tests (`api/py/`)
| File | Description |
|------|-------------|
| `test/test_pyspark.py` | **NEW** - PySpark integration tests |
| `ai/chronon/repo/test_helpers.py` | **NEW** - Test utilities for JVM bridge testing |

### Scala (`spark/src/main/scala/ai/chronon/spark/`)
| File | Description |
|------|-------------|
| `PySparkUtils.scala` | JVM entry points: `parseGroupBy`, `parseJoin`, `parseStagingQuery`, `runGroupBy`, `runJoin`, `runStagingQuery`, various `getOptional` helpers |
| `GroupBy.scala` | Added `usingArrayList()` method for Py4J-friendly GroupBy construction |

### Scala Tests
| File | Description |
|------|-------------|
| `PySparkUtilsTest.scala` | **NEW** - Unit tests for all PySparkUtils methods |

### Other
| File | Description |
|------|-------------|
| `.circleci/config.yml` | Updated to build JARs and cache SBT dependencies for Python tests |
| `api/py/requirements/dev.in` | Added `pyspark>=3.0.0` dependency |

## Usage Examples

### Databricks
```python
from ai.chronon.pyspark.databricks import DatabricksGroupBy, DatabricksJoin

executable = DatabricksGroupBy(my_group_by, spark)
result_df = executable.run(start_date='20250101', end_date='20250107')
```

### Jupyter
```python
from ai.chronon.pyspark.jupyter import JupyterGroupBy, JupyterJoin

executable = JupyterGroupBy(
    my_group_by, spark,
    name_prefix="my_project",
    output_namespace="my_database"
)
result_df = executable.run(start_date='20250101', end_date='20250107')
```

### StagingQuery
```python
from ai.chronon.pyspark.jupyter import JupyterStagingQuery

executable = JupyterStagingQuery(my_staging_query, spark)
result_df = executable.run(
    end_date='20250107',
    step_days=7,
    enable_auto_expand=True
)
```

## Limitations

- **S3 source resolution** - Stripe-specific `S3Utils` removed; raw table paths must be used

## Testing

- [x] Scala compilation passes
- [x] Scalafmt check passes
- [x] Python lint (flake8) passes
- [x] Scala unit tests pass (18 tests in PySparkUtilsTest)
- [x] Python unit tests pass (including PySpark integration tests)

## Related

- **GitHub Issue:** #980
- **CHIP:** CHIP-10 (Chronon Improvement Proposal)
