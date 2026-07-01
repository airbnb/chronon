You are helping a Chronon user test feature definitions locally using the LocalTestingPlatform. This platform lets you execute GroupBy, Join, and StagingQuery definitions against user-provided DataFrames entirely in-process — no cluster, no Hive metastore, no Airflow needed.

For help *writing* GroupBy or Join definitions, use `/user/groupby` or `/user/join` instead. This skill is about *executing and validating* them locally.

## Prerequisites

- `chronon-ai` Python package installed (provides `ai.chronon.*`)
- `pyspark` and `typing-extensions` installed
- `CHRONON_SPARK_JAR` environment variable pointing to the Chronon Spark deploy JAR
- `JAVA_HOME` set to a JDK installation

## API

```python
from ai.chronon.pyspark.local import (
    run_local_group_by,
    run_local_join,
    run_local_staging_query,
    reset_local_session,
)
```

### run_local_group_by

```python
result_df = run_local_group_by(
    group_by=my_gb,                          # A GroupBy object
    tables={"data.purchases": purchases_df}, # Table name -> DataFrame mapping
    start_date="20220101",                   # YYYYMMDD format
    end_date="20220107",
    step_days=30,                            # Optional, default 30
)
```

### run_local_join

```python
result_df = run_local_join(
    join=my_join,                            # A Join object
    tables={                                 # ALL tables referenced by left + right GroupBys
        "data.purchases": purchases_df,
        "data.returns": returns_df,
        "data.checkouts": checkouts_df,
    },
    start_date="20220101",
    end_date="20220107",
)
```

### run_local_staging_query

```python
result_df = run_local_staging_query(
    staging_query=my_sq,                     # A StagingQuery object
    tables={"raw.events": events_df},
    end_date="20220107",
)
```

### reset_local_session

Call `reset_local_session()` to tear down the SparkSession and clear all registered tables. The next `run_local_*` call will create a fresh session. Use this between unrelated test runs or when you want a clean slate.

## Automatic Naming

The platform automatically sets `metaData.name` and `metaData.team` when your definitions are imported from module files following Chronon's directory conventions. Your GroupBys should live under a `group_bys/` directory and Joins under a `joins/` directory. When you do `from joins.my_team.checkout_features import v1`, the name is derived from the module path automatically — no manual naming needed.

## Critical Rules

### 1. DataFrames must have a `ds` partition column

Every DataFrame passed in `tables` must have a `ds` column (string, format `YYYYMMDD`). This is used for Hive table partitioning.

### 2. Table names must match Source definitions

The keys in the `tables` dict must exactly match the table names in the Source definitions (e.g. if the Source says `table="data.purchases"`, the key must be `"data.purchases"`).

### 3. Timestamps are milliseconds

The `ts` column in event DataFrames must be Unix timestamps in **milliseconds** (not seconds). For example: `1640995200000` for `2022-01-01 00:00:00 UTC`.

### 4. EntitySource snapshots need rows per partition

For EntitySource (snapshot tables), provide rows for each `ds` partition in your date range. Unlike EventSource which accumulates events, EntitySource expects a complete snapshot per day.

## Workflow

### Step 1: Write feature definitions

Write GroupBy and Join definitions in your repo's feature directories (e.g. `group_bys/`, `joins/`). Use `/user/groupby` and `/user/join` for guidance.

### Step 2: Write a test file

Create a test file that imports your definitions, creates DataFrames from sample data, and runs the local platform.

### Step 3: Run and iterate

Run the test with pytest or your build system. Inspect results, modify definitions, re-run.

## Complete Working Example

Given a repo with this structure:
```
sources/
    events.py
group_bys/
    my_team/
        purchases.py
        returns.py
joins/
    my_team/
        checkout_features.py
tests/
    test_checkout_features.py
```

**sources/events.py**:
```python
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select

purchases_source = Source(
    events=EventSource(
        table="data.purchases",
        query=Query(
            selects=select("user_id", "purchase_price"),
            time_column="ts",
        ),
    )
)

returns_source = Source(
    events=EventSource(
        table="data.returns",
        query=Query(
            selects=select("user_id", "refund_amt"),
            time_column="ts",
        ),
    )
)

checkouts_source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=select("user_id"),
            time_column="ts",
        ),
    )
)
```

**group_bys/my_team/purchases.py**:
```python
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit
from sources.events import purchases_source

window_sizes = [Window(length=d, timeUnit=TimeUnit.DAYS) for d in [7, 30]]

v1 = GroupBy(
    sources=[purchases_source],
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="purchase_price", operation=Operation.SUM, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.COUNT, windows=window_sizes),
    ],
)
```

**group_bys/my_team/returns.py**:
```python
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit
from sources.events import returns_source

window_sizes = [Window(length=d, timeUnit=TimeUnit.DAYS) for d in [7, 30]]

v1 = GroupBy(
    sources=[returns_source],
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="refund_amt", operation=Operation.SUM, windows=window_sizes),
    ],
)
```

**joins/my_team/checkout_features.py**:
```python
from ai.chronon.join import Join, JoinPart
from sources.events import checkouts_source
from group_bys.my_team.purchases import v1 as purchases_v1
from group_bys.my_team.returns import v1 as returns_v1

v1 = Join(
    left=checkouts_source,
    right_parts=[
        JoinPart(group_by=purchases_v1),
        JoinPart(group_by=returns_v1),
    ],
)
```

**tests/test_checkout_features.py**:
```python
import pytest
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from ai.chronon.pyspark.local import (
    _get_or_create_session,
    run_local_join,
    reset_local_session,
)
from joins.my_team.checkout_features import v1 as checkout_join


@pytest.fixture(scope="module", autouse=True)
def cleanup():
    yield
    reset_local_session()


def test_checkout_features():
    spark = _get_or_create_session()

    checkouts_df = spark.createDataFrame(
        [("user_a", 1641168000000, "20220103"),
         ("user_b", 1641168000000, "20220103")],
        StructType([
            StructField("user_id", StringType()),
            StructField("ts", LongType()),
            StructField("ds", StringType()),
        ]),
    )

    purchases_df = spark.createDataFrame(
        [("user_a", 10.50, 1640995200000, "20220101"),
         ("user_b", 25.00, 1640995300000, "20220101"),
         ("user_a", 15.75, 1641081600000, "20220102"),
         ("user_a", 30.00, 1641081700000, "20220102")],
        StructType([
            StructField("user_id", StringType()),
            StructField("purchase_price", DoubleType()),
            StructField("ts", LongType()),
            StructField("ds", StringType()),
        ]),
    )

    returns_df = spark.createDataFrame(
        [("user_a", 10.50, 1641081650000, "20220102")],
        StructType([
            StructField("user_id", StringType()),
            StructField("refund_amt", DoubleType()),
            StructField("ts", LongType()),
            StructField("ds", StringType()),
        ]),
    )

    result_df = run_local_join(
        join=checkout_join,
        tables={
            "data.checkouts": checkouts_df,
            "data.purchases": purchases_df,
            "data.returns": returns_df,
        },
        start_date="20220101",
        end_date="20220103",
    )

    assert result_df is not None
    assert result_df.count() == 2
    result_df.show(truncate=False)
```

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `RuntimeError: No Chronon JARs found` | `CHRONON_SPARK_JAR` not set or path doesn't exist | Set the env var to point to the Chronon Spark deploy JAR |
| `JAVA_GATEWAY_EXITED` | Java not found | Set `JAVA_HOME` to a JDK installation |
| `PYTHON_VERSION_MISMATCH` | Driver and worker using different Python versions | The platform auto-detects `sys.executable`; ensure your build system uses a consistent Python |
| `ValueError: missing the partition column` | DataFrame lacks `ds` column | Add a `ds` string column (format `YYYYMMDD`) to your DataFrame |
| `'JavaPackage' object is not callable` | JARs not on classpath correctly | Verify `CHRONON_SPARK_JAR` path exists and contains Chronon classes |
