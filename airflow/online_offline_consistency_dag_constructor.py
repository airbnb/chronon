from teams.ml_infra.projects.chronon.common.constants import ZIPLINE_PATH
from teams.ml_infra.projects.chronon.common import os_helpers

from airflow.models import DAG

from datetime import datetime, timedelta


def dag_constructor(conf, mode, conf_type, team_conf):
    return DAG(
        os_helpers.dag_names(conf, mode, conf_type),
        default_args=os_helpers.dag_default_args(
            team_conf,
            conf["metaData"]["team"],
            retries=1,
            retry_delay=timedelta(minutes=1),
        ),
        dagrun_timeout=timedelta(days=4),
        schedule_interval='@daily',
        start_date=datetime.strptime("2022-02-01", "%Y-%m-%d"),
        catchup=False
    )


all_dags = os_helpers.walk_and_define_tasks(
    "consistency-metrics-compute", "joins", ZIPLINE_PATH, dag_constructor, dags={})
g = globals()
g.update(all_dags)
