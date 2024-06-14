"""
Open Source Helper to walk and build dags.

Most methods are internal with the exception of dag_default_args and walk_and_define_tasks.

For the purposes of building DAGs, leverage these two methods.
"""
import json
import logging
import os
import re
from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor


NO_SKIP_MODES = ["stats-summary", "label-join"]
# For spark 3 migration into streaming client mode, we'll migrate per team.
STREAMING_CLIENT_TEAMS = [
    # todo
]


def dag_default_args(team_conf, team_name, dag_owner_override=None, **kwargs):
    """
    Default args for all dags. Extendable with custom kwargs.
    """
    base = team_conf["default"].copy()
    base.update(team_conf.get(team_name, {}))

    # find DAG owner
    user = base.get("user")
    default_dag_owner_str = "" # TODO

    airflow_base = {
        "owner": default_dag_owner_str,
        "start_date": base.get("dag_start_date"),
        "email": base.get("maintainer_emails"),
        "hive_cli_conn_id": base.get("hive_cli_conn_id"),
        "queue": base.get("airflow_queue"),
        "run_as_user": user,
        "metastore_conn_id": "metastore_default",
        "presto_conn_id": "presto_default",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "task_concurrency": 1,
        "retries": 1,
    }
    default_args = kwargs.copy()
    airflow_base.update(kwargs)
    return airflow_base


def normalize_name(object_name):
    """Eliminate characters that would be problematic on task names"""
    time_parts = ["ds", "ts", "hr"]

    def safe_part(p):
        return not any(
            [p.startswith("{}=".format(time_part)) for time_part in time_parts]
        )

    safe_name = "__".join(filter(safe_part, object_name.split("/")))
    return re.sub("[^A-Za-z0-9_]", "__", safe_name)


# https://github.com/airbnb/chronon/blob/master/api/src/main/scala/ai/chronon/api/Extensions.scala
def sanitize(name):
    return re.sub("[^a-zA-Z0-9_]", "_", name)


def output_table(meta_data):
    return f"{meta_data['outputNamespace']}.{sanitize(meta_data['name'])}"


def logged_table(meta_data):
    return output_table(meta_data) + "_logged"


def requires_log_flattening_task(conf):
    return conf["metaData"].get("samplePercent", 0) > 0


def get_custom_json(data):
    return json.loads(data.get("metaData", {}).get("customJson", "{}"))


def get_offline_schedule(conf):
    schedule_interval = conf["metaData"].get("offlineSchedule", "@daily")
    if schedule_interval == "@never":
        return None
    return schedule_interval


def get_label_offline_schedule(conf):
    if conf.get("labelPart") is None:
        return None
    else:
        schedule_interval = conf["labelPart"]["metaData"].get(
            "offlineSchedule", "@daily"
        )
        if schedule_interval == "@never":
            return None
        return schedule_interval


def requires_label_join_task(conf):
    return conf.get("labelPart") is not None


def requires_frontfill(conf):
    return get_offline_schedule(conf) is not None


def requires_streaming_task(conf, conf_type):
    """
    Conditions for requiring streaming task.
    - A streaming source is set (topic or mutationTopic).
    - Accuracy is not set to Snapshot. (can be None and we infer it's streaming.
    """
    if conf_type == "group_bys":
        return any([requires_streaming_task_from_sources(conf["sources"])])
    return False


def requires_streaming_task_from_sources(sources):
    for source in sources:
        if (
            source.get("entities", {}).get("mutationTopic") is not None
            or source.get("events", {}).get("topic") is not None
            or requires_streaming_task_for_join_source(source)
        ):
            return True
    return False


def requires_streaming_task_for_join_source(source):
    """Find if there's topic or mutationTopic for underlying join in JoinSource"""
    join_conf = source.get("joinSource", {}).get("join")
    if join_conf is not None:
        return any(
            [
                requires_streaming_task_from_sources(
                    jps.get("groupBy", {}).get("sources")
                )
                for jps in join_conf["joinParts"]
            ]
            + [requires_streaming_task_from_sources([join_conf.get("left")])]
        )
    return False


def should_schedule(conf, mode, conf_type):
    """Based on a conf and mode determine if a conf should define a task."""
    if conf_type == "group_bys":
        if mode == "backfill":
            return conf.get("backfillStartDate") is not None
        if mode == "upload":
            return conf["metaData"].get("online", 0) == 1
        if mode.startswith("streaming"):
            # online + (realtime or has topic)
            online = conf["metaData"].get("online", 0) == 1
            streaming = requires_streaming_task(conf, conf_type)
            to_schedule = online and streaming
            # Split for client streaming or cluster mode streaming.
            streaming_client = conf["metaData"]["team"] in STREAMING_CLIENT_TEAMS
            if mode == "streaming-client":
                return streaming_client and to_schedule
            return not streaming_client and to_schedule
    if conf_type == "joins":
        if mode == "metadata-upload":
            return True
        if mode == "backfill":
            return requires_frontfill(conf)
        if mode == "stats-summary":
            return requires_frontfill(conf)
        if mode == "log-summary":
            return requires_log_flattening_task(conf)
        if mode == "consistency-metrics-compute":
            customJson = json.loads(conf["metaData"]["customJson"])
            return customJson.get("check_consistency") is True
        if mode == "log-flattener":
            return requires_log_flattening_task(conf)
        if mode == "label-join":
            return requires_label_join_task(conf)
        return False
    if conf_type == "staging_queries":
        return mode == "backfill"
    logging.warning(
        f"[Chronon][Schedule] Ignoring task for: {mode} {conf_type} {conf['metaData']['name']}"
    )


def extract_dependencies(conf, mode, conf_type, common_env, dag):
    """
    Build sensors for dependencies of a conf

    These tasks have custom skip.
    - GroupBy Upload
    - GroupBy Backfill
    - Staging Query
    - Online Offline Consistency
    - Join Backfill
    - Join log-flattener
    """
    if conf_type == "joins" and mode == "stats-summary":
        dependencies = [
            {
                "name": f"wf_{sanitize(output_table(conf['metaData']))}",
                "spec": f"{output_table(conf['metaData'])}/ds={{{{ ds }}}}",
            }
        ]
    elif conf_type == "joins" and mode == "log-summary":
        dependencies = [
            {
                "name": f"wf_{sanitize(logged_table(conf['metaData']))}",
                "spec": f"{logged_table(conf['metaData'])}/ds={{{{ ds }}}}",
            },
        ]
    elif mode == "consistency-metrics-compute":
        table_name = logged_table(conf["metaData"])
        dependencies = [
            {"name": "wf_flattened_log_table", "spec": f"{table_name}/ds={{{{ ds }}}}"}
        ]
    elif mode.startswith("streaming"):
        # Streaming has no dependencies as it's purpose is a check if job is alive.
        return [], []
    elif conf_type == "staging_queries":
        # Staging Queries have special dependency syntax.
        dependencies = [
            {"name": f"wf__{dep}", "spec": dep}
            for dep in conf["metaData"].get("dependencies", [])
        ]
    elif conf_type == "joins" and mode == "log-flattener":
        dependencies = [
            {
                # wait for SCHEMA_PUBLISH_EVENT partition which guarantees to exist every day
                "name": "wf_raw_log_table",
                "spec": f'{common_env["CHRONON_LOG_TABLE"]}/ds={{{{ ds }}}}/name=SCHEMA_PUBLISH_EVENT',
            },
            {
                "name": "wf_schema_table",
                "spec": f'{common_env["CHRONON_SCHEMA_TABLE"]}/ds={{{{ ds }}}}',
            },
        ]
    elif conf_type == "joins" and mode == "label-join":
        dependencies = [
            json.loads(dep)
            for dep in conf["labelPart"]["metaData"].get("dependencies", [])
        ]
    else:
        dependencies = [
            json.loads(dep) for dep in conf["metaData"].get("dependencies", [])
        ]
    operators = set()
    for dep in dependencies:
        name = normalize_name(dep["name"])
        if name in dag.task_dict:
            operators.add(dag.task_dict[name])
            continue
        op = (
            SensorWithEndDate(
                task_id=f"{name}_{dep['end'].replace('-','_')}",
                partition_names=[dep["spec"]],
                params={"end_partition": dep["end"]},
                execution_timeout=timedelta(hours=48),
                dag=dag,
                retries=3,
            )
            if dep.get("end")
            else NamedHivePartitionSensor(
                task_id=name,
                partition_names=[dep["spec"]],
                execution_timeout=timedelta(hours=48),
                dag=dag,
                retries=3,
            )
        )
        operators.add(op)

    if mode in NO_SKIP_MODES or (get_custom_json(conf).get("no_skip_mode") is not None):
        return operators, None
    skip_name = f"custom_skip__{normalize_name(conf['metaData']['name'])}"
    if skip_name in dag.task_dict:
        return operators, dag.task_dict[skip_name]
    custom_skip_op = create_skip_operator(dag, normalize_name(conf["metaData"]["name"]))
    operators >> custom_skip_op
    return operators, custom_skip_op


def get_downstream(conf, mode, conf_type, team_conf, dag):
    """
    Define the custom downstream tasks. Ex:
        GroupBy:
            upload -> Mussel export and upload.
        Join Backfill:
            upload join stats -> Mussel export and upload
        Join Log Flattening:
            upload log stats -> Mussel export and upload
        Join Consistency Check:
            upload consistency_check -> mussel export and upload

    """
    if conf_type == "group_bys" and mode == "upload":
        if conf["metaData"].get("online", 0) == 1:
            encoded_export = mussel_helper.create_mussel_group_by_upload_export(
                dag, conf, team_conf
            )
            encoded_export >> mussel_helper.upload_mussel_group_by(dag, conf, team_conf)
            return encoded_export
    if conf_type == "joins" and mode == "stats-summary":
        encoded_export = mussel_helper.create_mussel_stats_upload_export(
            dag,
            conf,
            team_conf,
            input_table=f'{output_table(conf["metaData"])}_daily_stats_upload',
            key_prefix="CHRONON_STATS_BATCH",
            table_suffix="_stats",
        )
        encoded_export >> mussel_helper.upload_mussel_stats(
            dag, conf, team_conf, table_suffix="_stats"
        )
        return encoded_export
    if conf_type == "joins" and mode == "consistency-metrics-compute":
        encoded_export = mussel_helper.create_mussel_stats_upload_export(
            dag,
            conf,
            team_conf,
            input_table=f'{output_table(conf["metaData"])}_consistency_upload',
            key_prefix="CHRONON_CONSISTENCY_METRICS_STATS_BATCH",
            table_suffix="_ooc_stats",
        )
        encoded_export >> mussel_helper.upload_mussel_stats(
            dag, conf, team_conf, table_suffix="_ooc_stats"
        )
        return encoded_export
    if conf_type == "joins" and mode == "log-summary":
        encoded_export = mussel_helper.create_mussel_stats_upload_export(
            dag,
            conf,
            team_conf,
            input_table=f'{logged_table(conf["metaData"])}_daily_stats_upload',
            key_prefix="CHRONON_LOG_STATS_BATCH",
            table_suffix="_log_stats",
        )
        encoded_export >> mussel_helper.upload_mussel_stats(
            dag, conf, team_conf, table_suffix="_log_stats"
        )
        return encoded_export
    return None


def get_extra_args(mode, conf_type, common_env, conf):
    args = {}
    if conf_type == "joins" and mode == "log-flattener":
        args.update(
            {
                "log-table": common_env["CHRONON_LOG_TABLE"],
                "schema-table": common_env["CHRONON_SCHEMA_TABLE"],
            }
        )
    if conf["metaData"]["team"] == "zipline_test":
        args.update(
            {
                # Set Staging Jar of chronon-online for zipline_test.
                "online-jar-fetch": os.path.join(
                    constants.ZIPLINE_PATH, "scripts/fetch_online_staging_jar.py"
                ),
            }
        )
    return args


def dag_names(conf, mode, conf_type):
    if conf_type == "joins" and mode == "metadata-upload":
        return "chronon_metadata_upload"
    if mode == "metadata-export":
        return "chronon_ums_metadata_export"
    team = conf["metaData"]["team"]
    name = normalize_name(conf["metaData"]["name"])
    schedule_interval = (
        ""
        if (
            get_offline_schedule(conf) is None or get_offline_schedule(conf) == "@daily"
        )
        else "_" + get_offline_schedule(conf)[1:]
    )
    # Group By
    if conf_type == "group_bys":
        if mode in ("upload", "backfill"):
            return f"chronon_group_by_batch_{team}{schedule_interval}"
        if mode.startswith("streaming"):
            return f"chronon_group_by_{mode.replace('-', '_')}_{team}"
    # Join
    if conf_type == "joins":
        if mode == "backfill":
            return f"chronon_join_{name}{schedule_interval}"
        if mode == "stats-summary":
            return "chronon_stats_compute"
        if mode == "log-flattener":
            return f"chronon_log_flattening_{team}"
        if mode == "consistency-metrics-compute":
            return f"chronon_online_offline_comparison_{name}"
        if mode == "label-join":
            return f"chronon_label_join_{name}"
        if mode == "log-summary":
            return "chronon_stats_compute"
    # Staging Query
    if conf_type == "staging_queries":
        if mode == "backfill":
            return f"chronon_staging_query_batch_{team}{schedule_interval}"
    raise ValueError(
        f"Unable to define proper DAG name:\nconf_type: {conf_type}\nmode: {mode}\nconf: {json.dumps(conf, indent=2)}"
    )


def task_names(conf, mode, conf_type):
    if conf_type == "joins" and mode == "metadata-upload":
        return "zipline_join_metadata_upload"
    if conf_type == "group_bys" and mode == "metadata-upload":
        return "zipline_group_by_metadata_upload"
    if mode == "metadata-export":
        return "zipline_ums_metadata_export"
    name = normalize_name(conf["metaData"]["name"])
    # Group By Tasks
    if conf_type == "group_bys":
        if mode == "upload":
            return f"group_by_batch__{name}"
        if mode == "backfill":
            return f"group_by_batch_backfill__{name}"
        if mode.startswith("streaming"):
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
        if mode == "label-join":
            return f"compute_labels__{name}"
        if mode == "log-summary":
            return f"log_summary__{name}"
    # Staging Query
    if conf_type == "staging_queries":
        if mode == "backfill":
            return f"staging_query__{name}"
    raise ValueError(
        f"Unable to define proper task name:\nconf_type: {conf_type}\nmode: {mode}\nconf: {json.dumps(conf, indent=2)}"
    )


def build_root_task(mode, dag):
    """
    Build or return the root operator for the dag
    In the case of streaming-client we are going to have a "forever" sensor to prevent the dag from falling into a
    failing state.
    Only address the cases when the operator is not found, most cases just return the task_id.
    """
    root_task_id = "all_start"
    if mode == "streaming-client":
        if root_task_id not in dag.task_dict:
            return NamedHivePartitionSensor(
                task_id=root_task_id,
                partition_names=[
                    "zipline.ml_infra_afp_logging_schema_v1/ds=not_exists"
                ],
                timeout=604800000,
                dag=dag,
            )
    else:
        if root_task_id not in dag.task_dict:
            return DummyOperator(task_id=root_task_id, dag=dag)
    return dag.task_dict[root_task_id]


def walk_and_define_tasks(
    mode, conf_type, repo, dag_constructor, dags=None, silent=True
):
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
            with open(full_path, "r") as infile:
                try:
                    conf = json.load(infile)
                    # Basic Key Check to guarantee it's a Chronon Json.
                    assert (
                        conf.get("metaData", {}).get("name") is not None
                        and conf.get("metaData", {}).get("team") is not None
                    )
                    if should_schedule(conf, mode, conf_type):
                        # Create DAG if not yet created.
                        dag_name = dag_names(conf, mode, conf_type)
                        if dag_name not in dags:
                            dags[dag_name] = dag_constructor(
                                conf, mode, conf_type, team_conf
                            )
                        dag = dags[dag_name]
                        # Build Chronon Operator
                        conf_path = os.path.relpath(full_path, repo)
                        params = {
                            "production": conf["metaData"].get("production", False),
                            "name": conf["metaData"]["name"],
                            "team": conf["metaData"]["team"],
                            "mode": mode,
                            "conf_path": full_path,
                            "conf": conf,
                            "team_conf": team_conf.get(conf["metaData"]["team"]),
                            "conf_type": conf_type,
                        }
                        base_team_conf = team_conf["default"]
                        common_env = base_team_conf["common_env"]
                        custom_json = json.loads(
                            conf["metaData"].get("customJson", "{}")
                        )
                        # Define SLA
                        sla = None
                        if custom_json.get(f"sla_{mode}"):
                            try:
                                sla = timedelta(hours=int(custom_json[f"sla_{mode}"]))
                            except TypeError as e:
                                pass
                        base_user = base_team_conf.get("user")
                        user = team_conf.get(conf["metaData"]["team"]).get(
                            "user", base_user
                        )
                        baseop = ChrononOperator(
                            conf_path,
                            mode,
                            repo,
                            conf_type,
                            extra_args=get_extra_args(
                                mode, conf_type, common_env, conf
                            ),
                            task_id=task_names(conf, mode, conf_type),
                            on_success_callback=monitoring.on_success_callback,
                            on_failure_callback=monitoring.update_datadog_counter_callback(
                                conf, conf_type, "chronon.airflow.failure", mode=mode
                            ),
                            on_retry_callback=monitoring.update_datadog_counter_callback(
                                conf, conf_type, "chronon.airflow.retry", mode=mode
                            ),
                            sla=sla,
                            params=params,
                            dag=dag,
                            # todo: refactor the code to request resources from driver memory in spark settings
                            resources=custom_json["resources"]
                            if "resources" in custom_json
                            else None,
                            run_as_user=user,
                        )
                        # Build Upstream dependencies (Hive)
                        sensors, custom_skips = extract_dependencies(
                            conf, mode, conf_type, common_env, dag
                        )
                        if custom_skips:
                            custom_skips >> baseop
                        else:
                            sensors >> baseop
                        # Build Downstream dependencies.
                        downstream = get_downstream(
                            conf, mode, conf_type, team_conf, dag
                        )
                        if downstream:
                            baseop >> downstream

                        # create a root node for easy restart
                        root_op = build_root_task(mode, dag)
                        root_op >> sensors

                except json.JSONDecodeError as e:
                    if not silent:
                        logger.exception(e)
                        logger.warning(f"Ignoring invalid json file: {name}")
                except AssertionError as x:
                    if not silent:
                        logger.warning(
                            f"[Chronon] Ignoring {conf_type} config as does not have required metaData: {name}"
                        )
    return dags
