# Deploying Chronon Entities

Once you have authored and tested a Chronon entity, such as a `GroupBy`, `Join` or `StagingQuery`, then all you need to do to productionize it is merge it into your repository. From there, the requisite DAGs and tasks will automatically get created to compute feature values and keep them up to date. See the [Orchestration](../setup/Orchestration.md) documentation for how to set this up.

## GroupBy

Once merged into production, `GroupBy`s will get the following tasks and dags:

1. (if online=True) A task in the `{team_name}_chronon_batch` DAG for daily batch uploads to the KV store.
2. (if online=True and streaming) A task in the `{team_name}_chronon_realtime` DAG for a streaming job that writes to the KV store.
3. (if backfill_start_date is set) A task in the `{team_name}_chronon_batch` DAG to front-fill the `GroupBy` snapshot table.

## Join

Once merged into production, `Join`s will get the following tasks and dags:

1. (if online=True) Then the metadata for this join will get updloaded to the KV store as part of the `chronon_metadata_upload` DAG (all joins share the same task).
2. (if check_consistency=True) A DAG gets created with the name pattern: `online_offline_comparison_<team_name>_<join_name>` which writes out daily consistency metrics as new upstream data lands.

## StagingQuery

Once merged into production, `StagingQuery`s will get the following tasks and dags:

1. A task in the `{team_name}_chronon_staging_query` DAG for daily computation of the staging query.
