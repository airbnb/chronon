# Bootstrap

We support feature **bootstrap** as a primitive as part of Chronon Join in order to support various kinds of feature experimentation workflows that are manually done by clients previously outside of Chronon.

Bootstrap is a preprocessing step in the **Join** job that enriches the left side with precomputed feature data, before running the regular group by backfills if necessary.

## How does bootstrap work?

When a join job is run, it backfills all features against the keys/timestamp of the left table. When bootstrap parts
are defined in the join, before it runs the backfill step, it joins the left table with the bootstrap tables on the
`row_ids`. Then for each following backfill, it checks whether the rows on the
left are already covered by the bootstrap. If so, then a backfill is skipped; if not, then a backfill is scheduled.

Bootstrap can happen both for a subset of rows and a subset of columns. Chronon will automatically backfill the rest
of rows or columns that are not covered by bootstrap.


## Scenarios
1. Log stitching: Use online serving log as the source for production features instead of continuously recomputing it offline to ensure online/offline consistency and reduce computation cost
2. External feature backfills: backfill external features from custom hive table before they are logged from online fetching
3. Data sharing across Joins:
   1. Reuse feature data from other Joins of the same left events 
   2. Maintain separate Joins for production vs experiments: bootstrap an experimental Join from existing production Join data
   3. Unblocks the possibility of 1:1 mapping between Join and ML Model
4. Key backfills: when adding new features with new keys, we need to change the left table to add the new keys. Often if we use log as the left side, those keys do not exist in logs, so a key backfill needs to be done first. It is better that we can maintain this flow within the Join API
5. Data recovery: fix broken log data during periods of serving outage using manual backfill
6. Data migration: migrate training data from legacy system

## API Example
**Generate feature data from online logging and offline backfills**

```python
my_model = Join(
    left=<my_driver_table>,
    right_parts=<my_features>
    # Define row-level identifiers for your offline dataset
    row_ids=["my_event_id"],
    # Send row_ids during online fetching to make the logs joinable offline
    online_external_parts=[
      ExternalPart(ContextualSource(
         fields=[("my_event_id", DataType.STRING)]
      )
    ],
    # Use log table as a bootstrap source to skip offline backfill
    bootstrap_from_log=True
)

```

## Bootstrap Table

Bootstrap table is a precomputed table which contains precomputed feature values for (subsets of) left table.

- It must have the same row id columns with the left table. row_ids is a newly introduced parameter at join level, and the purpose is to uniquely identify each row of the left table.
- Its (transformed) column names must be matching the output column name of the group-bys or external parts of a join. Transformation can be done on a raw table using Select in the API.
- Bootstrap can contain only a subset of rows, and Chronon will compute the rest of the rows from scratch
- Bootstrap can contain only a subset of columns, and Chronon will compute the rest of the columns  from scratch

## API w/ Examples

`payments_driver.v1` is a StagingQuery based on the log table of v1 Join, optionally unioned with historical data before logging is available source_v1 is a HiveEventSource based on this StagingQuery, used as `left` of v1 Join


```python

ENTITY_KEYS = ['guest', 'host', 'listing']

v1_source = EventSource(
	table=get_staging_query_output_table_name(payments_driver.v1),
	query=Query(
		selects=select(
			rng='rng',
			**{pk: pk for pk in ENTITY_KEYS}
		)
	)
)
```


`v1` Join presents a production model feature set
`left` is a wrapped table based on `v1`’s own logging table and historical data
`V1_FEATURES`/`V1_EXT_FEATURES` are file-level definitions to be shared across joins
`bootstrap_from_log=True` enables bootstrap from `v1` join’s own logging table

```python
V1_FEATURES = [...]
V1_EXT_FEATURES = [
    ExternalPart(
        ContextualSource(
            fields=[
                ("request_id", DataType.STRING),
                ...
            ]
        )
    ),
    ExternalPart(...),
    ...
]
ROW_IDS = ['request_id']

v1 = Join(
    online=True,
    left=v1_source,
    right_parts=V1_FEATURES,
    online_external_parts=V1_EXT_FEATURES,
    bootstrap_from_log=True,
    row_ids=ROW_IDS
)
```

To enable the next round of feature experimentation, we decide to create a new source based on the output table of `v1` join. 
In particular, we would like the label information from `v1` join’s output table in order to run downsampling on the training examples. 

Note: `rng` is an example for a column that clients can include in the StagingQuery `payments_driver.v1` in order to facilitate downsampling for sampling purpose and persisted for multiple runs 

```python
v2_dev_source = EventSource(
    table=get_join_output_table_name(v1),
    query=Query(
         wheres=['label = 1 OR rng < 0.05'],
    )
)

V2_FEATURES = [...]
```

`v2_join` is a join created purely for offline experimental features, and its output table will contain production features from v1 join and new experimental features. For production features, we reuse data `v1` join without recomputation via bootstrap

```python
v2_dev = Join(
	online=False,
	left=v2_dev_source,
	right_parts=V1_FEATURES+V2_FEATURES,
	bootstrap_parts=[
		BootstrapPart(
			table=get_join_output_table_name(v1, full_name=True)
		)
	]
)
```

After experimentation we are ready to productionize and make v2 online.
Similar to `v1_source`, we create a `v2_source` based on a new StagingQuery `payments_driver.v2`

```python
v2_source = EventSource(
	table=get_staging_query_output_table_name(payments_driver.v2),
	query=Query(
		selects=select(
			rng='rng',
			**{pk: pk for pk in ENTITY_KEYS}
		)
	)
)
```


`v2` is the join to serve the updated feature set (containing v1 and v2 features)
Note it has 3 bootstrapping component:
- `bootstrap_from_log` will populating ongoing data from logging table
- `get_join_output_table_name(v1)` will carry-over production features data from v1
- `get_join_output_table_name(v2_dev)` will carry-over partially backfilled v2 features
during experimentation on 5% of downsampled data. The rest of 95% of data will be automatically backfilled when this join is run for the first time. This behavior is called “automatic hole filling”. 

```python
v2 = Join(
	online=True,
	left=v2_source,
	right_parts=V1_FEATURES+V2_FEATURES,
	bootstrap_from_log=True,
	bootstrap_parts=[
		BootstrapPart(
			table=get_join_output_table_name(v1, full_name=True)
		),
		BootstrapPart(
			table=get_join_output_table_name(v2_dev, full_name=True)
		)
	]
)
```

## Clarifications

**When should I put bootstrap_parts into the join definition?**

Bootstrap is a critical step in the computation of join data, so we recommend that you carefully think through the `bootstrap_parts` definition at the same time when you begin to define the join itself.

Think about the data sources of your offline data:
- What are the different types of features you use? Are there production features or are all features experimental (i.e. new)?
- For production features: Where are they defined? Where and when are they logged?
- For experimental features: Which parts of the rows do I want to backfill (every row? subset?)

**Should I create a new join or modify an existing join?**

There are no hard rules on how many joins you can create for your ML use cases. ODM is designed to support creating and maintaining many joins in the most efficient way.

It is recommended that each time you train and/or serve a new model version, you **define a new join** that contains the exact list of features for that new model version. The Bootstrap API allows you to copy data from old joins and skip re-computing data from scratch.

Alternatively, you can also **modify an existing join** to update the feature list, but note that modification takes place very soon after you merge the changes, so in the scenarios where you want to keep multiple versions running (e.g. shadow-scoring, ERFs, etc.), updating in place may not be a good idea.

**How many joins should I keep in one Python file?**

It is recommended to keep all joins for the same ML use case in the same file so that it is easier to maintain and keep track of shared components such as feature list. That said, there is no restriction on how joins are defined in Chronon.

**Can I delete a join?**

Yes you can. When you delete a join variable, make sure to also delete its associated compiled json file. What happens next is that:
- Online, you will eventually lose the ability to fetch this join online; this does not happen immediately as we rely on a long TTL to purge metadata from the online system.
- Offline; Airflow DAGs won’t be scheduled for this join anymore (if the schedule was turned on), so Airflow will stop creating new partitions to the output table. The table and historical partitions are kept intact. Standard data retention still applies.

**How to define the left source of a join?**

We leave clients to define the left source of a join to allow maximum flexibility in handling potential scenarios. This however will cause some inconvenience to the clients if they are not familiar with the behavior of Chronon.

In essence, the left source should contain the **list of (row ids, entity keys, timestamp)** that serve as the base for ML training examples:
- Row ids are primary identifiers for a training row. Usually this is a request id for online requests. It should be passed as a contextual feature in the join to support log bootstrap
- Entity keys and timestamp are the join keys for group bys and external parts

**How to set up log-based training data generation properly?**

Chronon supports logging of online requests data (keys and values) into a Jitney stream, and automatically converts that into a flattened log table Hive table where each field (key/value) is stored in a column.

Log-based training data generation is the idea to leverage this workflow and the bootstrap functionality to stitch log data and backfill data together. To do that, you should:
- Define the row ids for the join use case.
  - Usually this is the unique id for your online requests (e.g. `service_request_id`)
- Pass row ids as a contextual feature, so that it can logged and attached to flattened log table to make it joinable
- Include row ids on the left source
  - It is recommended to use flattened log table to construct the left source for this reason
- Set `bootstrap_from_log=True`

**What if a bootstrap source table only covers a subset of group-bys?**
Bootstrap works by leveraging data as much as it can. It will skip the backfill and utilize bootstrap data for the subset of group-bys covered, and run backfills for the other group-bys.

**What if a bootstrap source table only covers a subset of rows for a particular ds?**  

Bootstrap works by leveraging data as much as it can. It will skip the backfill and utilize bootstrap data for the subset of rows already covered by the bootstrap source table, and run backfills for the other subset.

**What if a bootstrap source table only covers a subset of all features of a particular group-by?**

Similar to other scenarios, bootstrap will leverage data as much as it can. However, due to how Chronon group-by computation works, each group-by is backfilled as a single unit, therefore computation wise there is no saving. However, we will only use the backfilled values for the fields that are not already covered by bootstrap, i.e. this is done at individual column level.

**What happens if there are multiple bootstrap parts that contain the same field?**

When there are multiple bootstrap parts, the higher an item in the list, the higher it takes priority. If two bootstrap parts share the same column, we take the bootstrap data from the higher priority source first; if the value is NULL, then we take value from the lower priority source. Same rule applies when there are more than two bootstrap parts. Also, note that the log table always has the lowest priority (when bootstrap_from_log is enabled).

**How does bootstrap for external parts work?**

External parts by definition do not have a built-in backfill mechanism in Chronon. There are two ways for clients to supply values for external parts:
1. During online fetching, the Chronon fetcher will handle external parts fetching and will log the fetched external parts into a flattened log table together with native group-bys. These values can then be included in the join table through bootstrap_from_log.
2. External parts can be bootstrapped from arbitrary tables built by clients. Clients are responsible for building the data pipeline to construct the table from scratch.
   1. It is also possible to leverage Chronon to build these bootstrap source tables. In some edge-case scenarios in which Chronon native Group Bys cannot meet the serving SLAs for latency or freshness, but nevertheless you can still express the backfill logic using Chronon group bys and joins. We call these backfill-only group bys and joins. Clients can make the output table of a backfill-only join as the bootstrap source of the main join. 
