You are helping a Chronon user write a StagingQuery definition.

## Context

StagingQuery allows arbitrary Spark SQL transformations to prepare data before it's used in GroupBys. Use it for complex data prep that can't be expressed in Source queries.

## Key Files to Reference

- **API**: `api/py/ai/chronon/staging_query.py`
- **Examples**: `api/py/test/sample/staging_queries/`
- **Documentation**: `docs/source/authoring_features/StagingQuery.md`

## When to Use StagingQuery

- Complex SQL transformations (multiple joins, CTEs, window functions)
- Data cleaning/normalization
- Creating intermediate tables for multiple downstream GroupBys
- Pre-aggregating data before further aggregation
- Combining data from multiple source tables

## Basic Structure

```python
from ai.chronon.api.ttypes import StagingQuery, MetaData

v1 = StagingQuery(
    query="""
        SELECT
            user_id,
            transaction_id,
            amount,
            CAST(timestamp AS BIGINT) as ts,
            ds
        FROM raw_data.transactions
        WHERE status = 'completed'
          AND ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    """,
    metaData=MetaData(
        name="cleaned_transactions",
        outputNamespace="staging_db",
    ),
)
```

## Complete Example with Joins

```python
from ai.chronon.api.ttypes import StagingQuery, MetaData

v1 = StagingQuery(
    query="""
        WITH user_transactions AS (
            SELECT
                t.user_id,
                t.amount,
                t.timestamp as ts,
                u.country,
                u.account_type,
                t.ds
            FROM raw_data.transactions t
            JOIN raw_data.users u ON t.user_id = u.user_id
            WHERE t.status = 'completed'
              AND t.ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
        ),
        enriched AS (
            SELECT
                *,
                SUM(amount) OVER (
                    PARTITION BY user_id
                    ORDER BY ts
                    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
                ) as rolling_sum_10
            FROM user_transactions
        )
        SELECT * FROM enriched
    """,
    startPartition="2024-01-01",  # First partition to compute
    setups=[
        # Optional: Register UDFs if needed
        # "CREATE TEMPORARY FUNCTION my_udf AS 'com.example.MyUdf'",
    ],
    metaData=MetaData(
        name="enriched_transactions",
        outputNamespace="features_staging",
        dependencies=[
            "raw_data.transactions/ds={{ ds }}",
            "raw_data.users/ds={{ ds }}",
        ],
    ),
)
```

## Using StagingQuery Output in GroupBy

```python
# In staging_queries/team/enriched_transactions.py
v1 = StagingQuery(
    query="...",
    metaData=MetaData(
        name="enriched_transactions",
        outputNamespace="features_staging",
    ),
)

# In group_bys/team/user_features.py
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select

source = Source(
    events=EventSource(
        table="features_staging.enriched_transactions",  # Reference staging output
        query=Query(
            selects=select("user_id", "amount", "rolling_sum_10"),
            time_column="ts"
        )
    )
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[...],
)
```

## Key Parameters

| Parameter | Description |
|-----------|-------------|
| `query` | Spark SQL query string (use `{{ start_date }}` and `{{ end_date }}` for date range) |
| `startPartition` | First partition to compute (format: `YYYY-MM-DD`) |
| `setups` | List of SQL setup statements (e.g., UDF registration) |
| `createView` | If `True`, creates a view instead of a table (default: `False`) |
| `metaData.name` | Output table name |
| `metaData.outputNamespace` | Database/namespace for output table |
| `metaData.dependencies` | List of upstream table dependencies (format: `namespace.table/ds={{ ds }}`) |
| `metaData.tableProperties` | Optional table properties dict |

## Best Practices

1. **Idempotency**: Queries should be re-runnable without side effects
2. **Date Filtering**: Use `{{ start_date }}` and `{{ end_date }}` templates for incremental processing
3. **Full Paths**: Reference source tables with `namespace.table` format
4. **Type Safety**: Explicitly cast types, especially timestamps
5. **Comments**: Document complex logic in SQL comments

## Execution

```bash
# Compile the staging query
compile.py --conf=staging_queries/team/enriched_transactions.py

# Run the staging query
run.py --mode staging-query --conf production/staging_queries/team/enriched_transactions.v1 --ds 2024-01-01
```

## When Helping Users

1. Understand if StagingQuery is the right tool (vs. inline Query transformations)
2. Help structure complex SQL with CTEs for readability
3. Ensure proper type handling (especially for timestamps)
4. Recommend partitioning strategy for large datasets
5. Verify downstream GroupBys correctly reference the staging output

Read the key files listed above to provide specific, accurate guidance based on their question.
