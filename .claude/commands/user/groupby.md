You are helping a Chronon user write or modify a GroupBy definition.

## Context

GroupBy is the primary API for defining features in Chronon. It specifies aggregations computed from data sources, grouped by keys, over time windows.

## Key Files to Reference

- **API**: `api/py/ai/chronon/group_by.py`
- **Examples**: `api/py/test/sample/group_bys/`
- **Documentation**: `docs/source/authoring_features/GroupBy.md`

## Source Types

### EventSource
For event/log data with timestamps:
```python
Source(
    events=EventSource(
        table="data.purchases",
        topic="events.purchases",  # Optional: enables real-time streaming
        query=Query(
            selects=select("user_id", "amount"),
            time_column="ts"  # Required for temporal aggregations
        )
    )
)
```

### EntitySource
For snapshot/dimension data (daily partitioned):
```python
Source(
    entities=EntitySource(
        snapshotTable="data.users",
        query=Query(
            selects=select("user_id", "account_created_ds", "email_verified")
        )
    )
)
```

## Common Aggregations

| Category | Operations |
|----------|-----------|
| Basic | `COUNT`, `SUM`, `AVERAGE`, `MIN`, `MAX`, `VARIANCE` |
| Time-based | `FIRST`, `LAST`, `FIRST_K(n)`, `LAST_K(n)` |
| Approximate | `APPROX_UNIQUE_COUNT`, `APPROX_PERCENTILE([0.5, 0.95])`, `APPROX_HISTOGRAM_K(k)` |
| Collection | `TOP_K(n)`, `BOTTOM_K(n)`, `HISTOGRAM` |

## Windows

```python
from ai.chronon.group_by import Window, TimeUnit

# Define windows
windows = [
    Window(length=3, timeUnit=TimeUnit.DAYS),
    Window(length=14, timeUnit=TimeUnit.DAYS),
    Window(length=30, timeUnit=TimeUnit.DAYS),
]

# Unwindowed (lifetime) aggregation - omit windows parameter
Aggregation(input_column="amount", operation=Operation.SUM)

# Windowed aggregation
Aggregation(input_column="amount", operation=Operation.SUM, windows=windows)
```

## Complete Example

```python
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit
from ai.chronon.query import Query, select
from ai.chronon.api.ttypes import Source, EventSource

source = Source(
    events=EventSource(
        table="data.purchases",
        query=Query(
            selects=select("user_id", "purchase_price"),
            time_column="ts"
        )
    )
)

window_sizes = [Window(length=d, timeUnit=TimeUnit.DAYS) for d in [3, 14, 30]]

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,  # Enable online serving
    aggregations=[
        Aggregation(input_column="purchase_price", operation=Operation.SUM, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.COUNT, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.AVERAGE, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.LAST_K(10)),
    ],
)
```

## Key Considerations

1. **Source Type**: Use EventSource for logs/events, EntitySource for snapshots
2. **Time Column**: Required for EventSource with temporal aggregations
3. **Accuracy**: TEMPORAL (real-time) vs SNAPSHOT (midnight) - defaults based on topic presence
4. **Online Flag**: Set `online=True` only when ready for production serving
5. **Buckets**: Use for multi-dimensional aggregations (produces map output)

## When Helping Users

1. First understand their data source and what they're trying to compute
2. Determine if they need aggregations or direct field extraction
3. Choose appropriate operations based on data types
4. Recommend windows based on their use case
5. Consider online vs offline requirements

Read the key files listed above to provide specific, accurate guidance based on their question.
