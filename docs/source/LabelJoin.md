# Label Join

## API Example
**Generate training data by combining features & labels**

```python
"""
:param labels: List of labels
:param left_start_offset: Relative integer to define the earliest date label should be refreshed
compared to label_ds date specified. For labels with aggregations,
    this param has to be same as aggregation window size.
:param left_end_offset: Relative integer to define the most recent date(inclusive) label should be refreshed.
e.g. left_end_offset = 3 most recent label available will be 3 days
prior to 'label_ds' (including `label_ds`). For labels with aggregations, this param
has to be same as aggregation window size.
:param label_offline_schedule: Cron expression for Airflow to schedule a DAG for offline
label join compute tasks
"""

my_model = Join(
   ...,
   # Define label table and add it to the training set 
   label_part=LabelPart(
      labels=[JoinPart(group_by=GroupBy(
         name="my_label",
         sources=[HiveEntitySource(
            namespace="db_name",
            table="<my_label_table>",
            query=Query(selects=select(
               label_col="label_col",
               entity_id="entity_id"))
         )],
         aggregations=None,
         keys=["<entity_id>"]
      ))],
      left_start_offset=30,
      left_end_offset=0,
   )
)
```

## How does label join work?
Including a label_part to your join now creates a new set of workflows to create label tables and a labeled training
set view for the join. Internally, chronon (1) joins the label source table with the left table to create a materialized
label table, and (2) maintains a Hive view that joins the materialized label table (which doesn’t have feature data)
with the feature table. The (2) result is not materialized for cost efficiency consideration. Label join will also have
a dedicated DAG for label computation. In another word, for offline pipelines, you will have two workflows, one for join
backfill and the other for label join job.

## What do output tables look like?
```
 # output tables
 # 1. Regular join feature backfill table
 # 2. Joined view with features and labels. On the fly and not materialized
 # 3. Joined view with feature and latest labels available. On the fly and not materialized
 my_team_my_join_v1 
 my_team_my_join_v1_labeled
 my_team_my_join_v1_labeled_latest

# sample schema of my_team_my_join_v1_labeled

                  Column                   |  Type   | Extra | Comment
-------------------------------------------+---------+-------+---------
 key_1                                     | bigint  |       |key
 date_timestamp                            | varchar |       |date
 key_2                                     | bigint  |       |key
 key_3                                     | bigint  |       |key
 feature_col_1                             | varchar |       |feature
 feature_col_2                             | varchar |       |feature
 feature_col_3                             | varchar |       |feature
 label_col_1                               | integer |       |label
 label_col_2                               | integer |       |label
 label_col_3                               | integer |       |label
 label_ds                                  | varchar |       |label version

# sample schema of my_team_my_join_v1_labeled_latest. Same as above. 
# If a particular date do have multiple label versions like 2023-01-24, 2023-02-01, 2023-02-08, only the latest label would show up in this view which is .

                  Column                   |  Type   | Extra | Comment
-------------------------------------------+---------+-------+---------
 key_1                                     | bigint  |       |key
 date_timestamp                            | varchar |       |ds
 key_2                                     | bigint  |       |key
 key_3                                     | bigint  |       |key
 feature_col_1                             | varchar |       |feature
 feature_col_2                             | varchar |       |feature
 feature_col_3                             | varchar |       |feature
 label_col_1                               | integer |       |label
 label_col_2                               | integer |       |label
 label_col_3                               | integer |       |label
 label_ds                                  | varchar |       |label version
