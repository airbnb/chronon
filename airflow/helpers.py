"""
Helper to walk files and build dags.
"""

from operators import ChrononOperator, create_skip_operator, SensorWithEndDate
import constants

from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from datetime import timedelta
import logging
import json
import os
import re
from datetime import datetime, timedelta

def task_default_args(team_conf, team_name, **kwargs):
    """
    Default args for all dags. Extendable with custom kwargs.
    """
    base = team_conf["default"].copy()
    base.update(team_conf.get(team_name, {}))
    airflow_base = {
        'owner': base.get('team_name'),
        "queue": base.get('airflow_queue'),
        'start_date': base.get('dag_start_date'),
        'email': base.get('maintainer_emails'),
        'hive_cli_conn_id': base.get('hive_cli_conn_id'),
        'queue': base.get("airflow_queue"),
        'run_as_user': base.get('user'),
        'metastore_conn_id': 'metastore_default',
        'presto_conn_id': 'presto_default',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'task_concurrency': 1,
        'retries': 1
    }
    airflow_base.update(kwargs)
    return airflow_base


def dag_default_args(**kwargs):
    return {
        'start_date': datetime.strptime("2023-02-01", "%Y-%m-%d"),
        'dagrun_timeout': timedelta(days=4),
        'schedule_interval': '@daily',
        'concurrency': BATCH_CONCURRENCY,
        'catchup': False,
    }.update(kwargs)

def get_kv_store_upload_operator(dag, conf, team_conf):
    """TODO: Your internal implementation"""
    return


def normalize_name(object_name):
    """Eliminate characters that would be problematic on task names"""

    def safe_part(p):
        return not any([
            p.startswith("{}=".format(time_part))
            for time_part in constants.time_parts
        ])

    safe_name = "__".join(filter(safe_part, object_name.split("/")))
    return re.sub("[^A-Za-z0-9_]", "__", safe_name)


# https://github.com/airbnb/chronon/blob/main/api/src/main/scala/ai/chronon/api/Extensions.scala
def sanitize(name):
    return re.sub("[^a-zA-Z0-9_]", "_", name)


def output_table(meta_data):
    return f"{meta_data['outputNamespace']}.{sanitize(meta_data['name'])}"


def logged_table(meta_data):
    return output_table(meta_data) + "_logged"


def requires_log_flattening_task(conf):
    return conf["metaData"].get("samplePercent", 0) > 0


def get_offline_schedule(conf):
    schedule_interval = conf["metaData"].get("offlineSchedule", "@daily")
    if schedule_interval == "@never":
        return None
    return schedule_interval


def requires_frontfill(conf):
    return get_offline_schedule(conf) is not None


def requires_streaming_task(conf, conf_type):
    """Find if there's topic or mutationTopic for a source helps define streaming tasks"""
    if conf_type == "group_bys":
        return any([
            source.get("entities", {}).get("mutationTopic") is not None or
            source.get("events", {}).get("topic") is not None
            for source in conf["sources"]
        ])
    return False


def should_schedule(conf, mode, conf_type):
    """Based on a conf and mode determine if a conf should define a task."""
    if conf_type == "group_bys":
        if mode == "backfill":
            return conf.get("backfillStartDate") is not None
        if mode == "upload":
            return conf["metaData"].get("online", 0) == 1
        if mode == "streaming":
            # online + (realtime or has topic)
            online = conf["metaData"].get("online", 0) == 1
            streaming = requires_streaming_task(conf, conf_type)
            return conf["metaData"].get("online", 0) == 1 and (
                conf["metaData"].get("accuracy", 1) == 0 or
                requires_streaming_task(conf, conf_type)
            )
    if conf_type == "joins":
        if mode == "metadata-upload":
            return True
        if mode == "backfill":
            return requires_frontfill(conf)
        if mode == 'stats-summary':
            return requires_frontfill(conf)
        if mode == "consistency-metrics-compute":
            customJson = json.loads(conf["metaData"]["customJson"])
            return customJson.get("check_consistency") is True
        if mode == "log-flattener":
            return requires_log_flattening_task(conf)
        return False
    if conf_type == "staging_queries":
        return mode == "backfill"
    logging.warning(f"[Chronon][Schedule] Ignoring task for: {mode} {conf_type} {conf['metaData']['name']}")


def extract_dependencies(conf, mode, conf_type, common_env, dag):
    """
    Build sensors for dependencies of a conf

    These tasks have custom skip.
    - GroupBy Upload
    - GroupBy Backfill
    - Staging Query
    - Online Offline Consistency
    - Join Backfill
    """

    if conf_type == 'joins' and mode == "stats-summary":
        dependencies = [{
            "name": f"wf_{sanitize(output_table(conf['metaData']))}",
            "spec": f"{output_table(conf['metaData'])}/ds={{{{ ds }}}}",
        }]
    elif mode == "consistency-metrics-compute":
        table_name = logged_table(conf["metaData"])
        dependencies = [{'name': 'wf_flattened_log_table', 'spec': f'{table_name}/ds={{{{ ds }}}}'}]
    elif mode == "streaming":
        # Streaming has no dependencies as it's purpose is a check if job is alive.
        return []
    elif conf_type == "staging_queries":
        # Staging Queries have special dependency syntax.
        dependencies = [{"name": f"wf__{dep}", "spec": dep} for dep in conf["metaData"].get("dependencies", [])]
    elif conf_type == "joins" and mode == "log-flattener":
        dependencies = [
            {
                # wait for SCHEMA_PUBLISH_EVENT partition which guarantees to exist every day
                'name': f'wf_raw_log_table',
                'spec': f'{common_env["CHRONON_LOG_TABLE"]}/ds={{{{ ds }}}}/name=SCHEMA_PUBLISH_EVENT',
            },
            {
                'name': 'wf_schema_table',
                'spec': f'{common_env["CHRONON_SCHEMA_TABLE"]}/ds={{{{ ds }}}}'
            }
        ]
    else:
        dependencies = [
            json.loads(dep) for dep in conf["metaData"].get('dependencies', [])
        ]
    operators = set()
    for dep in dependencies:
        name = normalize_name(dep["name"])
        if name in dag.task_dict:
            operators.add(dag.task_dict[name])
            continue
        op = SensorWithEndDate(
            task_id=name,
            partition_names=[dep["spec"]],
            params={"end_partition": dep["end"]},
            execution_timeout=timedelta(hours=48),
            dag=dag,
            retries=3,
        ) if dep.get("end") else NamedHivePartitionSensor(
            task_id=name,
            partition_names=[dep["spec"]],
            execution_timeout=timedelta(hours=48),
            dag=dag,
            retries=3,
        )
        operators.add(op)

    skip_name = f"custom_skip__{normalize_name(conf['metaData']['name'])}"
    if skip_name in dag.task_dict:
        return dag.task_dict[skip_name]
    custom_skip_op = create_skip_operator(dag, normalize_name(conf["metaData"]["name"]))
    operators >> custom_skip_op
    return custom_skip_op


def get_downstream(conf, mode, conf_type, team_conf, dag):
    """
    Define the custom downstream tasks. Ex:
        GroupBy:
            upload -> Mussel export and upload.
    """
    if conf_type == "group_bys" and mode == "upload":
        if conf["metaData"].get("online", 0) == 1:
            return get_kv_store_upload_operator(dag, conf, team_conf)
    return None


def get_extra_args(mode, conf_type, common_env, conf):
    args = {}
    if conf_type == "joins" and mode == "log-flattener":
        args.update({
            "log-table": common_env["CHRONON_LOG_TABLE"],
            "schema-table": common_env["CHRONON_SCHEMA_TABLE"]
        })
    if conf["metaData"]["team"] == constants.TEST_TEAM_NAME:
        args.update({
            "online-jar-fetch": os.path.join(constants.CHRONON_PATH, "scripts/fetch_online_staging_jar.py"),
        })
    return args


def dag_names(conf, mode, conf_type):
    if conf_type == "joins" and mode == "metadata-upload":
        return f"chronon_metadata_upload"
    if mode == "metadata-export":
        return f"chronon_ums_metadata_export"
    team = conf["metaData"]["team"]
    name = normalize_name(conf["metaData"]["name"])
    # Group By
    if conf_type == "group_bys":
        if mode in ("upload", "backfill"):
            return f"chronon_group_by_batch_{team}"
        if mode == "streaming":
            return f"chronon_group_by_streaming_{team}"
    # Join
    if conf_type == "joins":
        if mode == "backfill":
            return f"chronon_join_{name}"
        if mode == "stats-summary":
            return f"chronon_stats_compute"
        if mode == "log-flattener":
            return f"chronon_log_flattening_{team}"
        if mode == "consistency-metrics-compute":
            return f"chronon_online_offline_comparison_{name}"
    # Staging Query
    if conf_type == "staging_queries":
        if mode == "backfill":
            return f"chronon_staging_query_batch_{team}"
    raise ValueError(
        f"Unable to define proper DAG name:\nconf_type: {conf_type}\nmode: {mode}\nconf: {json.dumps(conf, indent=2)}")


def task_names(conf, mode, conf_type):
    if conf_type == "joins" and mode == "metadata-upload":
        return f"chronon_join_metadata_upload"
    name = normalize_name(conf["metaData"]["name"])
    # Group By Tasks
    if conf_type == "group_bys":
        if mode == "upload":
            return f"group_by_batch__{name}"
        if mode == "backfill":
            return f"group_by_batch_backfill__{name}"
        if mode == "streaming":
            return f"group_by_streaming__{name}"
    # Join Tasks
    if conf_type == "joins":
        if mode == "backfill":
            return f"compute_join__{name}"
        if mode == "consistency-metrics-compute":
            return f"compute_consistency__{name}"
        if mode == "stats-summary":
            return f"feature_stats__{name}"
        if mode == "log-flattener":
            return f"log_flattening__{name}"
    # Staging Query
    if conf_type == "staging_queries":
        if mode == "backfill":
            return f"staging_query__{name}"
    raise ValueError(
        f"Unable to define proper task name:\nconf_type: {conf_type}\nmode: {mode}\nconf: {json.dumps(conf, indent=2)}"
    )


def walk_and_define_tasks(mode, conf_type, repo, dag_constructor, dags=None, silent=True):
    """
    Walk a folder and define a DAG for each conf in there.
    """
    logger = logging.getLogger()
    base = os.path.join(repo, "production", conf_type)
    if not dags:
        dags = {}
    with open(os.path.join(repo, "teams.json")) as team_infile:
        team_conf = json.load(team_infile)
    for root, dirs, files in os.walk(base):
        for name in files:
            full_path = os.path.join(root, name)
            with open(full_path, 'r') as infile:
                try:
                    conf = json.load(infile)
                    # Basic Key Check to guarantee it's a Chronon Json.
                    assert (
                        conf.get("metaData", {}).get("name") is not None and
                        conf.get("metaData", {}).get("team") is not None)
                    if should_schedule(conf, mode, conf_type):
                        # Create DAG if not yet created.
                        dag_name = dag_names(conf, mode, conf_type)
                        if dag_name not in dags:
                            dags[dag_name] = dag_constructor(conf, mode, conf_type, team_conf)
                        dag = dags[dag_name]
                        # Build Chronon Operator
                        conf_path = os.path.relpath(full_path, repo)
                        params = {
                            "production": conf["metaData"].get("production", False),
                            "name": conf["metaData"]["name"],
                            "team": conf["metaData"]["team"],
                            "conf_path": full_path,
                            "conf": conf,
                            "team_conf": team_conf.get(conf["metaData"]["team"]),
                            "conf_type": conf_type,
                        }
                        common_env = team_conf["default"]["common_env"]
                        baseop = ChrononOperator(
                            conf_path,
                            mode,
                            repo,
                            conf_type,
                            extra_args=get_extra_args(mode, conf_type, common_env, conf),
                            task_id=task_names(conf, mode, conf_type),
                            on_success_callback=monitoring.update_datadog_counter_callback(
                                conf, conf_type, "chronon.airflow.success", mode=mode),
                            on_failure_callback=monitoring.update_datadog_counter_callback(
                                conf, conf_type, "chronon.airflow.failure", mode=mode),
                            on_retry_callback=monitoring.update_datadog_counter_callback(
                                conf, conf_type, "chronon.airflow.retry", mode=mode),
                            params=params,
                            dag=dag
                        )
                        # Build Upstream dependencies (Hive)
                        dependencies = extract_dependencies(conf, mode, conf_type, common_env, dag)
                        dependencies >> baseop
                        # Build Downstream dependencies.
                        downstream = get_downstream(conf, mode, conf_type, team_conf, dag)
                        if downstream:
                            baseop >> downstream
                except json.JSONDecodeError as e:
                    if not silent:
                        logger.exception(e)
                        logger.warning(f"Ignoring invalid json file: {name}")
                except AssertionError as x:
                    if not silent:
                        logger.warning(
                            f"[Chronon] Ignoring {conf_type} config as does not have required metaData: {name}")
    return dags
