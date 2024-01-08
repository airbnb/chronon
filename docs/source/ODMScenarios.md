# Offline Data Management Scenarios Deep Dive

1. [Introduction](##Introduction)
2. Scenarios Deep Dive
   1. [A - Create a brand new feature set data](#A---Create-a-brand-new-feature-set)
   2. [B - Set up log-based data refresh for an online model](#B---Set-up-log-based-data-refresh-for-an-online-model)
   3. [C - Improve an existing feature set of an online model](#C---Improve-an-existing-feature-set-of-an-online-model)
   4. [D - Expand a model to a new set of drivers](#D---Expand-a-model-to-a-new-set-of-drivers)
   5. [E - Reuse existing feature data from the same drivers](#E---Reuse-existing-feature-data-from-the-same-drivers)
   6. [F - Utilize advanced features](#F---Utilize-advanced-features)
   7. [G - Leverage feature data from legacy data pipelines](#G---Leverage-feature-data-from-legacy-data-pipelines)
   8. [H - Overwrite incorrect logged feature values](#H---Overwrite-incorrect-logged-feature-values)
   9. [I - Adding labels to training dataset](#I---Adding-labels-to-training-dataset)
   10. [J - Adding labels and apply aggregation on labels](#J---Adding-labels-and-apply-aggregation-on-labels)
3. [FAQs](#FAQs)

## Introduction

ODM (offline data management) is a Chronon component that improves the overall experience of creating and managing offline datasets that are powering your ML workflows.

It consists of 3 core building blocks:

1. **Logging** - for capturing online production features
   Feature keys & values are now automatically logged and processed into offline tables for all online Chronon requests. You can utilize the offline log table for many purposes, such as ML observability and auto-retraining.

2. **Bootstrap** - for producing feature sets from across sources & environments  
   You can now create a new training set table from multiple data sources. For example, you can utilize logging for production features and backfill for experimental features; you can reuse backfilled data across multiple runs; you can even share feature values produced by other folks. No more backfills if the data is already available somewhere!

3. **Label computation** - for attaching labels to features to form the full training set
   You can now add labels to your features! Chronon now supports a wide range of labeling computation patterns, including both pre-aggregated labels, or labels that require windowed aggregations

### A - Create a brand-new feature set
Goal
- build the training set for a brand-new model and serve the same feature set online for inference

Steps

1. Create the driver table where each row represents a training example. This can be via a StagingQuery or directly from an existing hive table
2. Define and/or locate the group bys that you want to include in your model
3. Define the join which consists of your driver table on the left and the list of group bys on the right
4. Run **join backfill** on gateway, and train your model using the output table of the join
5. You can modify the join by changing the list of group bys, and rerun the join backfill. Chronon will try as much as possible to avoid unnecessary backfills across runs.
6. For online models, you can use this join for online serving in your service
7. For offline models, you can use this join to enable daily feature generation

```python
# ml_models/zipline/staging_queries/team_name/driver_table.py
v1 = StagingQuery(
  query="...",
)
# ml_models/zipline/joins/team_name/model.py
v1 = Join(
  # driver table can be either output of an staging_query or custom hive table
  left=HiveEventSource(
    namespace="db_name",
    table=get_staging_query_output_table_name(driver_table.v1),
    query=Query(...)
  )
  # all group_bys for the model for both backfill & serving
  right_parts=[
    JoinPart(group_by=feature_group_1.v1),
    JoinPart(group_by=feature_group_2.v1),
    ...
  ],
  # set this to @daily if you want to enable continuous backfill in a daily DAG
  offline_schedule='@never',
  # set this to True for online inference
  online=False
)
```

### B - Set up log-based data refresh for an online model
Goal
- Create a feature pipeline to automatically populate new ds using logged feature values

Steps

1. Update your driver table. This is optional but recommended that you follow the pattern to ensure bootstrap happens correctly.
   - In the following pattern, we take a union of the logged table and pre-logging driver table to handle data concatenation
     from two periods before and after logging. You should ensure that column selection aligns between the two periods.
   - One of the main considerations behind this pattern is to ensure a perfect matching between left and log table.
     If you use your own driver table, there is a chance that some rows are not covered by bootstrap. In those cases,
     AFP will run a regular backfill for those unfilled rows, which may not be what you want.
3. Define row_ids & register them as a contextual feature
   - **row_ids** defines the primary keys for each training example. It is also the join keys that will be used during
     log-based bootstrap. For online models this is often based on the event_id of your inference request.
   - You also should set **online_external_parts** in the join and include whatever you put in row_ids (except for columns
     that are already keys of some group bys). The goal is to pass these row_ids as contextual features during online serving so that they can be captured by logging and used to make the logs joinable offline.
4. Set **bootstrap_from_log** to True
   - This parameter tells Chronon to use the log table of the join as a bootstrap part. When you run the join backfill
     job, for features that are already logged, Chronon will run the bootstrap to pull values from logs and skip backfill.


```python
# ml_models/zipline/staging_queries/team_name/driver_table.py
v1 = StagingQuery(
  query="""
  WITH 
  log_drivers AS (
    SELECT event_id, <entity_keys>, ts, ds
    FROM (
      SELECT 
       event_id, <entity_keys>, ts, ds
       , ROW_NUMBER() OVER (PARTITION BY event_id, ds ORDER BY ts DESC) AS rank
      FROM <db_name>.<join_name>_logged
      WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
      AND ds > '<historical_data_end_date>'
    ) a
    WHERE rank = 1
  ),
  init_drivers AS (
    SELECT event_id, <entity_keys>, ts, ds
    FROM <db_name>.<driver_table_before_logging>
    WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND ds <= '<historical_data_end_date>'
  )
  SELECT *
  FROM log_drivers
  UNION ALL 
  SELECT *
  FROM init_drivers
  """
)
# ml_models/zipline/joins/team_name/model.py
v1 = Join(
  # it's important to use the SAME staging query before and after. 
  left=HiveEventSource(
    namespace="db_name",
    table=get_staging_query_output_table_name(driver_table.v1),
    query=Query(...)
  ),
  # all group_bys for the model for both backfill & serving
  right_parts=[
    JoinPart(group_by=feature_group_1.v1),
    JoinPart(group_by=feature_group_2.v1),
    
    ...
  ],
  # event_id has to be unique in order to facilitate bootstrap 
  row_ids=["event_id"],
  # event_id is captured from online serving & logging as a contextual feature
  online_external_parts=[
     ExternalPart(ContextualSource(
        fields=[("event_id", DataType.STRING)]
     )
  ],
  # set this to True to automatically pull values from logging table
  bootstrap_from_log=True,
  online=True,
)
```
It’s important to NOT change the left to a different table. It’s OK to change the underlying staging query logic, as long as the table name is unchanged. This is critical because if Chronon detects any changes on the left, it will treat it as a completely new join and archive the historical data.
If indeed something like that happened, or if you must use a different left table, consider creating a new join and adding the output of the previous join as a bootstrap part.
```python
v2 = Join(
  # if you must use a different driver table
  left=HiveEventSource(
    namespace="db_name",
    table=get_staging_query_output_table_name(driver_table.v2),
    query=Query(...)
  ),
  # carry over all other parameters from v1 join
  ...
  # add v1 table as a bootstrap part 
  bootstrap_parts=[BootstrapPart(table="db_name.team_name_model_v1")]
)
```
### C - Improve an existing feature set of an online model
Goal
- Create a new version of an existing model, carrying over most of the existing features while adding some new features.
  Steps
1. Define and/or locate the new group bys that you want to include in your model
2. Create a new instance of the join similar to the join of the existing model but with an updated list of group bys (including both old and new).
3. Include the existing join as a bootstrap_part in the new join so that all production feature values can be carried over to the new join without any backfill from scratch.
```python
# local variable to support sharing the same config values across two joins
right_parts_production = [...]
right_parts_experimental = [...]
driver_table = HiveEventSource(
  namespace="db_name",
  table=get_staging_query_output_table_name(driver_table.v1),
  query=Query(wheres=downsampling_filters)
)
# config for existing model in production 
v1 = Join(
  left=driver_table,
  right_parts=right_parts_production,
  ...
)
# config for next model for experimentation
v2 = Join(
  left=driver_table,
  right_parts=right_parts_production + right_parts_experimental,
  ...
  # include production join as a bootstrap_part
  bootstrap_parts=[
    BootstrapPart(table=get_join_output_table_name(v1, full_name=True))
  ]
)
```
### D - Expand a model to a new set of drivers
Goal
- Create a new version of an existing model that expands to new traffic endpoints
- This requires us to backfill features for the new endpoints in order to backtest the model, while keeping using log data for existing endpoints.
  Steps
1. Create a new driver table that contains the unioned data of new driver rows and old driver rows.
2. Create a new join with similar parameters except for the left where you would use the new driver table
3. Include the existing join as a bootstrap_part so that existing feature data can be carried over to the new join without any backfill from scratch
```python
# ml_models/zipline/staging_queries/team_name/driver_table.py
v2 = StagingQuery(
  query="""
	SELECT * 
	FROM db_name.team_name_driver_table_v1
      WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
      UNION ALL
	-- custom SQL logic for new driver segments
	SELECT ...
	FROM ...
  """
)
# ml_models/zipline/joins/team_name/model.py
# local variable to support sharing the same config values across two joins
right_parts = [...]
driver_table = HiveEventSource(
  namespace="db_name",
  table=get_staging_query_output_table_name(driver_table.v1),
  query=Query(wheres=downsampling_filters)
)
# config for existing model in production 
v1 = Join(
  left=driver_table,
  right_parts=right_parts,
  ...
)
# config for next model for experimentation
v2 = Join(
  left=driver_table,
  right_parts=right_parts,
  ...
  # include production join as a bootstrap_part
  bootstrap_parts=[
    BootstrapPart(table=get_join_output_table_name(v1, full_name=True))
  ]
)
```
### E - Reuse existing feature data from the same drivers
Goal:
- Build a new model while leveraging training data of an existing model that shares the same events.
  Step
1. Identify existing join that share the same set of drivers
2. Add that join as a bootstrap part in your join in order to reuse the data
```python
# ml_models/zipline/joins/team_name_a/model_a.py
v1 = Join(...)
# ml_models/zipline/joins/team_name_b/model_b.py
from joins.team_name_a import model_a
from ai.chronon.utils import get_join_output_table_name
v2 = Join(
...,
bootstrap_parts=[
	BootstrapPart(
        table=get_join_output_table_name(model_a.v1, full_name=True),
        query=Query(
            # select the list of features to reuse
            selects=select(
                feature_a="feature_a",
                feature_b="feature_b"
	            )
            )
        )
    ]
)
```
### F - Utilize advanced features
Goal:
- Leverage external / contextual features in the model. Once they are served online, we want to log them for future model retrains. For initial backfills, users will come up with a custom way to provide backfilled values
  Steps
1. Add online_external_parts to your join config, which includes defining the input and output schema of an external call
2. In your online service, implement the actual external call logic in an ExternalSourceHandler and register it to Chronon.
   See [AFP Orchestrator](https://docs.google.com/document/d/16ycE0GFLfRHJEauuWyz-3VWnO5--AI56WXT5xudKfos/edit#heading=h.kwh85a6763id) for more details
3. Since Chronon has no way to backfill the external features natively, instead we expect users to leverage bootstrap to
   ingest externally backfilled data for these external fields, such that they can be concatenated with other features
```python
v1 = Join(
   left=...,
   right_parts=...,
   online_external_parts=[
      # contextual fields including request id and request context string
      ExternalPart(ContextualSource(
         fields=[
            ("event_id", DataType.STRING),
            ("request_context", DataType.STRING)
         ]
      )),
      # external feature definition that calls userService to fetch user features
      ExternalPart(ExternalSource(
         name="service_user",
         team="team_name",
         key_fields=[
            ("user", DataType.LONG)
         ],
         value_fields=[
            ("country", DataType.STRING),
            ("phone_country", DataType.STRING),
      ))
   ],
   # bootstrap_part to ingest historical data for external fields
   bootstrap_parts=[
      BootstrapPart(
         table="<custom_external_backfill_table>",
         query=Query(
            # selected field names need to match feature names
            selects=select(
               request_context_id="...",
               ext_service_user_country="...",
               ext_service_user_phone_country="...",
            ),
            start_partition="...",
            end_partition="..."
         )
      )
   ]
)
```
### G - Leverage feature data from legacy data pipelines
Goal
- We have feature data from a legacy data pipeline before a certain cutoff, and while we are moving over to Chronon after that, we would like to retain and ingest the historical data into the final training table produced by Chronon.
  Steps
1. Create a driver table that contains the driver rows for the legacy data time period
2. Finalize the mapping table between legacy table’s column names with chronon’s output table column names, because usually they are different, and in bootstrap we expect the selected column names to be matching with output names
3. Register the legacy table as a bootstrap part in the join

```python
# ml_models/zipline/staging_queries/team_name/driver_table.py
v1 = StagingQuery(
  query="""
  WITH 
  legacy_drivers AS (
    SELECT ...
    FROM <legacy_table>
      ),
  new_drivers AS (
    SELECT ...
    FROM <legacy_table>
  )
  SELECT *
  FROM legacy_drivers
  UNION ALL 
  SELECT *
  FROM new_drivers
  """
)
# ml_models/zipline/joins/team_name/model.py
CHRONON_TO_LEGACY_NAME_MAPPING_DICT = {
	"chronon_output_column_name": "legacy_table_column_name", 
	...
}
v1 = Join(
  # driver table with union history
  left=HiveEventSource(
    namespace="db_name",
    table=get_staging_query_output_table_name(driver_table.v1),
    query=Query(...)
  ),
  right_parts=...
  # event_id has to be unique in order to facilitate bootstrap 
  row_ids=["event_id"],
  bootstrap_parts=[
	BootstrapPart(
		table="<legacy_table>",
		query=Query(selects=select(**CHRONON_TO_LEGACY_NAME_MAPPING_DICT))
	)
  ]
)
```
### H - Overwrite incorrect logged feature values
Goal
- The online serving was broken for certain feature values for a certain time period. We would like to overwrite the feature values by providing a custom backfill table.
  Step
1. For group by features, create a one-off join to backfill the values for those affect time period
2. For online external features, write a custom SQL query to build a backfill table.
3. Register the backfill table as a bootstrap part in the join
```python
backfill_2023_05_01 = Join(
   left=HiveEventSource(
      namespace="db_name",
      table=get_staging_query_output_table_name(driver_table.v1),
      query=Query(
         start_partition="2023-05-01",
         end_partition="2023-05-01"
      )),
   right_parts=[
      # specific subset of group bys to trigger manual backfill
      JoinPart(group_by=...)
   ]
)
v1 = Join(
   ...,
   bootstrap_parts=[
      BootstrapPart(table=get_join_output_table_name(
         backfill_2023_05_01, full_name=True))
   ]
)
```
### I - Adding labels to training dataset
Goal
- We have a backfilled feature table and label data available. We would like to associate label values to features and generate the training dataset.
  Steps
1. Identify the raw label data table(entity source), create one if not already available
2. Make sure to run join feature backfill before running label-join job. This is a dependency for label join airflow job and label job will only kick off once join feature tables are ready.
3. Create a new label part and specify the label source, label group bys, start & end offset. Label part group by can be defined in independent group by file similar to features.
4. Add label_part to the existing join model. This label_part will auto-generate an airflow job to compute labels and stitch label values with feature values in the format of Hive view.
5. Note that left_start_offset is a relative integer to define the earliest date label should be refreshed compared to label_ds date specified. And left_end_offset is for more recent date.
```python
label_part_group_by = GroupBy(
   name="sample_label_group_by",
   sources=test_sources.batch_entity_source,
   keys=["group_by_subject"],
   aggregations=None,
   online=False,
)
v2 = Join(
   left=test_sources.event_source,
   output_namespace="sample_namespace",
   right_parts=[
       JoinPart(
           group_by=event_sample_group_by.v1,
           key_mapping={'subject': 'group_by_subject'},
       ),
       JoinPart(
           group_by=group_by_with_kwargs.v1,
           key_mapping={'subject': 'group_by_subject'},
       ),
   ],
   label_part=LabelPart([
           JoinPart(
               group_by=label_part_group_by
           ),
       ],
       left_start_offset=30,
       left_end_offset=10,
       label_offline_schedule="@weekly"
   )
)
```
### J - Adding labels and apply aggregation on labels
Goal
- We have raw labels and would like to apply an aggregation to labels before adding these labels to  training datasets.
  Steps
1. Identify the raw label data table (event source), create one with label data if not already available
2. Make sure to run join feature backfill before running label-join job. This is a dependency for label join airflow job and label job will only kick off once join feature tables are ready.
3. Create a new label group by and specify the aggregation details. The aggregation concept is the same as existing group by feature aggregation.
4. Create label_part using label group by defined above and add label_part to the existing join model.
5. Note that single window aggregation is allowed and the start_offset and end_offset is required to be the same as window size.
   Given that we will refresh only the single-day label (as opposed to a range) once it has matured over the next window.  
```python
label_part_group_by = GroupBy(
   name="sample_label_group_by",
   sources=test_sources.entity_source,
   keys=["group_by_key"],
   aggregations=[
       Aggregation(input_column="group_by_key", operation=Operation.SUM, windows=[Window(7, TimeUnit.DAYS)]),
   ],
   online=False,
)
v2 = Join(
   left=test_sources.event_source,
   output_namespace="sample_namespace",
   right_parts=[
       JoinPart(
           group_by=event_sample_group_by.v1,
           key_mapping={'subject': 'group_by_key'},
       ),
       JoinPart(
           group_by=group_by_with_kwargs.v1,
           key_mapping={'subject': 'group_by_key'},
       ),
   ],
   label_part=LabelPart([
           JoinPart(
               group_by=label_part_group_by
           ),
       ],
       left_start_offset=7,
       left_end_offset=7,
       label_offline_schedule="@weekly"
   ),
)
```
## FAQs
### When should I put bootstrap_parts into the join definition?
Bootstrap is a critical step in the computation of join data, so we recommend that you carefully think through the bootstrap_parts definition at the same time when you begin to define the join itself.
Think about the data sources of your offline data:
1. What are the different types of features you use? Are there production features or are all features experimental (i.e. new)?
2. For production features: Where are they defined? Where and when are they logged?
3. For experimental features: Which parts of the rows do I want to backfill (every row? subset?)
### Should I create a new join or modify an existing join?
There are no hard rules on how many joins you can create for your ML use cases. ODM is designed to support creating
and maintaining many joins in the most efficient way.
It is recommended that each time you train and/or serve a new model version, you **define a new join** that contains the
exact list of features for that new model version. The Bootstrap API allows you to copy data from old joins and skip
re-computing data from scratch.
Alternatively, you can also **modify an existing join** to update the feature list, but note that modification takes
place very soon after you merge the changes, so in the scenarios where you want to keep multiple versions running (e.g. shadow-scoring, ERFs, etc.), updating in place may not be a good idea.
### How many joins should I keep in one Python file?
It is recommended to keep all joins for the same ML use case in the same file so that it is easier to maintain and keep track of shared components such as feature list.
That said, there is no restriction on how joins are defined in Chronon.
### Can I delete a join?
Yes you can. When you delete a join variable, make sure to also delete its associated compiled json file. What happens next is that:
- Online, you will eventually lose the ability to fetch this join online; this does not happen immediately as we rely on a long TTL to purge metadata from the online system.
- Offline; Airflow DAGs won’t be scheduled for this join anymore (if the schedule was turned on), so Airflow will stop creating new partitions to the output table. The table and historical partitions are kept intact. Standard data retention still applies.
### How to define the left source of a join?
We leave clients to define the left source of a join to allow maximum flexibility in handling potential scenarios.
This however will cause some inconvenience to the clients if they are not familiar with the behavior of Chronon.
In essence, the left source should contain the **list of (row ids, entity keys, timestamp)** that serve as the base for ML training examples:
- **Row ids** are primary identifiers for a training row. Usually this is a request id for online requests. It should be passed as a contextual feature in the join to support log bootstrap
- **Entity keys and timestamp** are the join keys for group bys and external parts
### How to set up log-based training data generation properly?
Chronon supports logging of online requests data (keys and values) into a data stream, and automatically converts that into a flattened log table Hive table where each field (key/value) is stored in a column.
Log-based training data generation is the idea to leverage this workflow and the bootstrap functionality to stitch log data and backfill data together. To do that, you should:
- Define the **row ids** for the join use case. Usually this is the online request id.
- Pass **row ids** as a contextual feature, so that it can logged and attached to **flattened log table** to make it joinable
- Include **row ids** on the left source
- It is recommended to use flattened log table to construct the left source for this reason
- Set **bootstrap_from_log=True**
### What if a bootstrap source table only covers a subset of group-bys?
Bootstrap works by leveraging data as much as it can. It will skip the backfill and utilize bootstrap data for the subset of group-bys covered, and run backfills for the other group-bys.
### What if a bootstrap source table only covers a subset of rows for a particular ds?
Bootstrap works by leveraging data as much as it can. It will skip the backfill and utilize bootstrap data for the subset of rows already covered by the bootstrap source table, and run backfills for the other subset.
### What if a bootstrap source table only covers a subset of all features of a particular group-by?
Similar to other scenarios, bootstrap will leverage data as much as it can. However, due to how Chronon group-by computation works, each group-by is backfilled as a single unit,
therefore computation wise there is no saving. However, we will only use the backfilled values for the fields that are not already covered by bootstrap, i.e.
this is done at individual column level.
### What happens if there are multiple bootstrap parts that contain the same field?
When there are multiple bootstrap parts, the higher an item in the list, the higher it takes priority. If two bootstrap
parts share the same column, we take the bootstrap data from the higher priority source first; if the value is NULL,
then we take value from the lower priority source. Same rule applies when there are more than two bootstrap parts.
Also, note that the log table always has the lowest priority (when bootstrap_from_log is enabled).
### How does bootstrap for external parts work?
External parts by definition do not have a built-in backfill mechanism in Chronon. There are two ways for clients to supply
values for external parts:
1. During online fetching, the Chronon fetcher will handle external parts fetching and will log the fetched external parts
   into a flattened log table together with native group-bys.
   These values can then be included in the join table through bootstrap_from_log.
2. External parts can be bootstrapped from arbitrary tables built by clients. Clients are responsible for building the data pipeline to construct the table from scratch.
   - It is also possible to leverage Chronon to build these bootstrap source tables. In some edge-case scenarios in which
     Chronon native Group Bys cannot meet the serving SLAs for latency or freshness, but nevertheless you can still express
     the backfill logic using Chronon group bys and joins. We call these backfill-only group bys and joins. Clients can make
     the output table of a backfill-only join as the bootstrap source of the main join. 
