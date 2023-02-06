import helpers
from constants import CHRONON_PATH, BATCH_CONCURRENCY
from airflow.models import DAG
from datetime import datetime, timedelta


def batch_constructor(conf, mode, conf_type, team_conf):
    return DAG(
        helpers.dag_names(conf, mode, conf_type),
        default_args=helpers.dag_default_args(
            team_conf,
            conf["metaData"]["team"],
            retries=1,
            retry_delay=timedelta(minutes=1),
        ),
        start_date=datetime.strptime("2022-02-01", "%Y-%m-%d"),
        dagrun_timeout=timedelta(days=4),
        schedule_interval='@daily',
        concurrency=BATCH_CONCURRENCY,
        catchup=False
    )


def streaming_constructor(conf, mode, conf_type, team_conf):
    return DAG(
        helpers.dag_names(conf, mode, conf_type),
        default_args=helpers.dag_default_args(
            team_conf,
            conf["metaData"]["team"],
            retries=1,
            retry_delay=timedelta(seconds=60),
            queue='silver_medium',
        ),
        start_date=datetime.strptime("2022-02-01", "%Y-%m-%d"),
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=20),
        schedule_interval=timedelta(minutes=20),
        catchup=False,
    )


all_dags = helpers.walk_and_define_tasks("streaming", "group_bys", CHRONON_PATH, streaming_constructor, dags={})
all_dags.update(
    helpers.walk_and_define_tasks("backfill", "group_bys", CHRONON_PATH, batch_constructor, dags=all_dags)
)
all_dags.update(
    helpers.walk_and_define_tasks("upload", "group_bys", CHRONON_PATH, batch_constructor, dags=all_dags)
)
g = globals()
g.update(all_dags)
