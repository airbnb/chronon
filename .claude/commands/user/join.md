You are helping a Chronon user write or modify a Join definition.

## Context

Join combines multiple GroupBys into a single dataset. Critically, it defines the timeline along which features are backfilled, ensuring **point-in-time correctness** for ML training data.

## Key Files to Reference

- **API**: `api/py/ai/chronon/join.py`
- **Examples**: `api/py/test/sample/joins/`
- **Documentation**: `docs/source/authoring_features/Join.md`

## Basic Structure

```python
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, select
from ai.chronon.api.ttypes import Source, EventSource

# Left side: drives the backfill timeline
left_source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=select("user_id"),  # Primary key(s)
            time_column="ts"            # Timeline for feature computation
        )
    )
)

v1 = Join(
    left=left_source,
    right_parts=[
        JoinPart(group_by=purchases_v1),
        JoinPart(group_by=returns_v1),
        JoinPart(group_by=users_v1),
    ],
    online=True,           # Enable online serving
    check_consistency=True # Compare online/offline values
)
```

## Left Side

The left side is crucial - it defines:
- **Primary keys** for joining GroupBys
- **Timeline** for backfill (features computed as-of these timestamps)

### EventSource Left
Features computed at **exact timestamps**:
```python
left = Source(events=EventSource(
    table="data.checkouts",
    query=Query(selects=select("user_id"), time_column="ts")
))
```

### EntitySource Left
Features computed at **midnight boundaries**:
```python
left = Source(entities=EntitySource(
    snapshotTable="data.daily_users",
    query=Query(selects=select("user_id"))
))
```

## JoinPart Options

### Basic
```python
JoinPart(group_by=my_group_by)
```

### Key Mapping
When key names differ between left and right:
```python
JoinPart(
    group_by=user_features,
    key_mapping={"buyer_id": "user_id"}  # left_key: right_key
)
```

### Prefix
Add prefix to output columns (useful for same GroupBy with different keys):
```python
JoinPart(group_by=user_features, prefix="buyer_")
JoinPart(group_by=user_features, key_mapping={"seller_id": "user_id"}, prefix="seller_")
```

## Label Joins

For training data with labels (labels come from the FUTURE):
```python
from ai.chronon.join import Join, JoinPart, LabelPart  # Note: import LabelPart

v1 = Join(
    left=left_source,
    right_parts=[...],
    label_part=LabelPart(
        labels=[JoinPart(group_by=label_group_by)],
        left_start_offset=30,  # Labels from 30 days after
        left_end_offset=0,
    )
)
```

## Complete Example

```python
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, select
from ai.chronon.api.ttypes import Source, EventSource

# Import GroupBys from other files
from group_bys.team.purchases import v1 as purchases_v1
from group_bys.team.returns import v1 as returns_v1
from group_bys.team.users import v1 as users_v1

source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=select("user_id"),
            time_column="ts"
        )
    )
)

v1 = Join(
    left=source,
    right_parts=[
        JoinPart(group_by=purchases_v1),
        JoinPart(group_by=returns_v1),
        JoinPart(group_by=users_v1),
    ],
    online=True,
    check_consistency=True,
    sample_percent=0.1,  # Sample 10% for consistency logging
)
```

## Key Considerations

1. **Left Side Purpose**: Only used for offline backfill, not online serving
2. **Point-in-Time**: Features computed as-of left timestamps (no future leakage)
3. **Key Availability**: All GroupBy keys must be available on the left side
4. **Consistency**: Enable `check_consistency=True` for production joins
5. **Versioning**: Create new versions (`v2`) instead of modifying online joins

## When Helping Users

1. Understand what event/entity drives their ML model (that's the left side)
2. Verify all right-side GroupBy keys are available on the left
3. Use key_mapping when key names don't match
4. Consider label joins for training data with future labels
5. Recommend consistency checking for production use

Read the key files listed above to provide specific, accurate guidance based on their question.
