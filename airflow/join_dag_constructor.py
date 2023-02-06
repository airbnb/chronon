import helpers, constants
from airflow.models import DAG
from datetime import datetime, timedelta


def dag_constructor(conf, mode, conf_type, team_conf):
    return DAG(
        helpers.dag_names(conf, mode, conf_type),
        **helpers.dag_default_args(
            concurrency=constants.JOIN_CONCURRENCY,
            schedule_interval=helpers.get_offline_schedule(conf)),
        default_args=helpers.task_default_args(
            team_conf,
            conf["metaData"]["team"],
            retries=1,
            retry_delay=timedelta(minutes=1),
            resources={"ram": 12288},
        ),
    )


join_dags = helpers.walk_and_define_tasks("backfill", "joins", constants.CHRONON_PATH, dag_constructor)
g = globals()
g.update(join_dags)
