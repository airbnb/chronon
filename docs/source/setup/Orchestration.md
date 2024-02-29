# Orchestrating Chronon

Orchestration for Chronon involves running the various jobs to compute batch and streaming feature computation, as well on online/offline consistency measurement.

Airflow is currently the best supported method for orchestration, however, other integrations are also possible.

## Airflow Integration

See the [Airflow Directory](https://github.com/airbnb/chronon/tree/main/airflow) for initial boilerplate code. 

The files in this directory can be used to create the following Chronon Airflow DAGs.

1. GroupBy DAGs, created by [group_by_dag_constructor.py](https://github.com/airbnb/chronon/tree/main/airflow/group_by_dag_constructor.py):
   1. `chronon_batch_dag_{team_name}`: One DAG per team that uploads snapshots of computed features to the KV store for online group_bys, and frontfills daily snapshots for group_bys.
   2. `chronon_streaming_dag_{team_name}`: One DAG per team that runs Streaming jobs for `online=True, realtime=True` GroupBys. These tasks run every 15 minutes and are configured to "keep alive" streaming jobs (i.e. do nothing if running, else attempt restart if dead/not started). 
2. Join DAGs, created by [join_dag_constructor.py](https://github.com/airbnb/chronon/tree/main/airflow/join_dag_constructor.py):
   1. `chronon_join_{join_name}`: One DAG per join that performs backfill and daily frontfill of join data to the offline Hive table.
3. Staging Query DAGs, created by [staging_query_dag_constructor.py](https://github.com/airbnb/chronon/tree/main/airflow/staging_query_dag_constructor.py):
   1. `chronon_staging_query_{team_name}`: One DAG per team that creates daily jobs for each Staging Query for the team.  
4. Online/Offline Consistency Check DAGs, created by [online_offline_consistency_dag_constructor.py](https://github.com/airbnb/chronon/tree/main/airflow/online_offline_consistency_dag_constructor.py):
   1. `chronon_online_offline_comparison_{join_name}`: One DAG per join that computes the consistency of online serving data vs offline data for that join, and outputs the measurements to a stats table for each join that is configured. Note that logging must be enabled for this pipeline to work.

To deploy this to your airflow environment, first copy everything in this directory over to your Airflow directory (where your other DAG files live), then set the following configurations:

1. Set your configuration variables in [constants.py](https://github.com/airbnb/chronon/tree/main/airflow/constants.py).
2. Implement the `get_kv_store_upload_operator` function in [helpers.py](https://github.com/airbnb/chronon/tree/main/airflow/helpers.py). **This is only required if you want to use Chronon online serving**.


## Alternate Integrations


While Airflow is currently the most well-supported integration, there is no reason why you couldn't choose a different orchestration engine to power the above flows. If you're interested in such an integration and you think that the community might benefit from your work, please consider [contributing](https://github.com/airbnb/chronon/blob/main/CONTRIBUTING.md) back to the project.

If you have questions about how to approach a different integration, feel free to ask for help in the [community Discord channel](https://discord.gg/GbmGATNqqP).
