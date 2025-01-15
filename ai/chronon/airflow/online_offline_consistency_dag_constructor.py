from constants import ZIPLINE_PATH
import helpers

from airflow.models import DAG

from datetime import datetime, timedelta


def dag_constructor(conf, mode, conf_type, team_conf):
    return DAG(
        os_helpers.dag_names(conf, mode, conf_type),
        **helpers.dag_default_args(),
        default_args=helpers.task_default_args(
            team_conf,
            conf["metaData"]["team"],
            retries=1,
            retry_delay=timedelta(minutes=1),
        ),
    )


all_dags = os_helpers.walk_and_define_tasks(
    "consistency-metrics-compute", "joins", ZIPLINE_PATH, dag_constructor, dags={})
g = globals()
g.update(all_dags)
