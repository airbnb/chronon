### Chronon Python API


#### Overview

Chronon Python API for materializing configs to be run by the Chronon Engine. Contains python helpers to help managed a repo of feature and join definitions to be executed by the chronon scala engine.


#### User API Overview

##### Sources

Most fields are self explanatory. Time columns are expected to be in milliseconds (unixtime).

```python
# File <repo>/sources/test_sources.py
from ai.chronon.query import (
  Query,
  select,
)
from ai.chronon.api.ttypes import Source, EventSource, EntitySource

# Sample query
Query(
  selects=select(
      user="user_id",
      created_at="created_at",
  ),
  wheres=["has_availability = 1"],
  start_partition="2021-01-01",  # Defines the beginning of time for computations related to the source.
  setups=["...UDF..."],
  time_column="ts",
  end_partition=None,
  mutation_time_column="mutation_timestamp",
  reversal_column="CASE WHEN mutation_type IN ('DELETE', 'UPDATE_BEFORE') THEN true ELSE false END"
)

user_activity = Source(entities=EntitySource(
  snapshotTable="db_exports.table",
  mutationTable="mutations_namespace.table_mutations",
  mutationTopic="mutationsKafkaTopic",
  query=Query(...)
)

website__views = Source(events=EventSource(
  table="namespace.table",
  topic="kafkaTopicForEvents",
)
```


##### Group By (Features)

Group Bys are aggregations over sources that define features. For example:

```python
# File <repo>/group_bys/example_team/example_group_by.py
from ai.chronon.group_by import (
  GroupBy,
  Window,
  TimeUnit,
  Accuracy,
  Operation,
  Aggregations,
  Aggregation,
  DefaultAggregation,
)
from sources import test_sources

sum_cols = [f"active_{x}_days" for x in [30, 90, 120]]


v0 = GroupBy(
  sources=test_source.user_activity,
  keys=["user"],
  aggregations=Aggregations(
    user_active_1_day=Aggregation(operation=Operation.LAST),
    second_feature=Aggregation(
      input_column="active_7_days",
      operation=Operation.SUM,
      windows=[
        Window(n, TimeUnit.DAYS) for n in [3, 5, 9]
      ]
    ),
  ) + [
    Aggregation(
      input_column=col,
      operation=Operation.SUM
    ) for col in sum_columns           # Alternative syntax for defining aggregations.
  ] + [
    Aggregation(
      input_column="device",
      operation=LAST_K(10)
    )
  ],
  dependencies=[
    "db_exports.table/ds={{ ds }}"      # If not defined will be derived from the Source info.
  ],
  accuracy=Accuracy.SNAPSHOT,          # This could be TEMPORAL for point in time correctness.
  env={
    "backfill": {                      # Execution environment variables for each of the modes for `run.py`
      "EXECUTOR_MEMORY": "4G"
     },
  },
  online=True,                         # True if this group by needs to be uploaded to a KV Store.
  production=False                     # True if this group by is production level.
)
```

##### Join

A Join is a collection of feature values for the keys and (times if applicable) defined on the left (source). Example:

```python
# File <repo>/joins/example_team/example_join.py
from ai.chronon.join import Join, JoinPart
from sources import test_sources
from group_bys.example_team import example_group_by

v1 = Join(
    left=test_sources.website__views,
    right_parts=[
        JoinPart(group_by=example_group_by.v0),
    ],
    online=True,       # True if this join will be fetched in production.
    production=False,  # True if this join should not use non-production group bys.
    env={"backfill": {"PARALLELISM": "10"}, "streaming": {"STREAMING_ENV_VAR": "VALUE"}},
)
```
