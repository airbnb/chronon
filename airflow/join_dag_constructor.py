from constants import CHRONON_PATH
import helpers
from airflow.models import DAG
from datetime import datetime, timedelta


def dag_constructor(conf, mode, conf_type, team_conf):
    return DAG(
        helpers.dag_names(conf, mode, conf_type),
        default_args=helpers.dag_default_args(
            team_conf,
            conf["metaData"]["team"],
            retries=1,
            retry_delay=timedelta(minutes=1),
            resources={"ram": 12288},
        ),
        start_date=datetime.strptime("2022-02-01", "%Y-%m-%d"),
        dagrun_timeout=timedelta(days=4),
        concurrency=100,
        schedule_interval=helpers.get_offline_schedule(conf),
        catchup=False
    )


join_dags = helpers.walk_and_define_tasks("backfill", "joins", CHRONON_PATH, dag_constructor)
g = globals()
g.update(join_dags)
