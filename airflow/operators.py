from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import TaskInstance, DagRun
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.utils.decorators import apply_defaults

import decorators

from datetime import datetime, timedelta
from urllib.request import urlretrieve
import getpass
import logging
import tarfile
import json
import os


class ChrononOperator(BashOperator):
    """
    Main Operator for running Chronon Jobs.
    Takes care of alerting handling, downloading the package and building the run.py command to execute.
    """
    REQUIRED_PARAMS = ["production", "team", "name"]
    API_VERSION = "0.0.20"
    CHRONON_VERSION = "0.0.20"

    def __init__(self, conf_path, mode, repo, conf_type, extra_args={}, *args, **kwargs):
        self.mode = mode
        self.conf_path = os.path.join(repo, conf_path)
        self.repo = repo
        self.conf_type = conf_type
        self.lag = 0
        extra_args_fmt = " ".join([f"--{arg_key} {arg_val}" for (arg_key, arg_val) in extra_args.items()])
        conf_arg = f"--conf={self.conf_path}" if conf_path else ""
        # Pinning 0.0.11 for streaming until TaskNotSerializable External registry is fixed.
        self.runpy_args = f"--version={self.CHRONON_VERSION} --mode={mode} {conf_arg} {extra_args_fmt}"
        # For UI rendering purposes or if chronon-ai in airflow environment.
        bash_command = f"python3 run.py {self.runpy_args} "
        env = {
            "CHRONON_REPO_PATH": repo,
            "USER": getpass.getuser(),
        }
        self.pre_execute_callback = None
        super(ChrononOperator, self).__init__(bash_command=bash_command, env=env, *args, **kwargs)
        if mode in ("metadata-upload", "fetch", "metadata-export"):
            return
        # Assertions on requirements for operator.
        for param in self.REQUIRED_PARAMS:
            assert param in self.params, (
                f"[Chronon] Missing required parameter in operator {param}: {self.param['conf_path']}")
        self.internal_specifics()

    @staticmethod
    @decorators.retry()
    def download_chronon_api_package(version):
        """
        Define a file to download the python package.
        Extract the package into a folder with user in path for permissions reasons.
        return the path to run.py
        """
        logger = logging.getLogger()
        whoami = getpass.getuser()
        download_file = f"chronon_{version}_{whoami}"
        output = os.path.join('/tmp/', download_file)
        run_file = os.path.join(f'chronon-ai-{version}', 'ai/chronon/repo/run.py')
        user_tmp_dir = f'/tmp/{whoami}'
        run_file_path = os.path.join(user_tmp_dir, run_file)
        logger.info(f"Checking for existing package at {output}")
        if not os.path.exists(run_file_path):
            download_url = f"https://pypi.io/packages/source/c/chronon-ai/chronon-ai-{version}.tar.gz"
            logger.info(f"downloading from : {download_url}")
            urlretrieve(download_url, filename=output)
            assert os.path.exists(output)
            logger.info(f"Finished downloading to: {output}")
            with tarfile.open(output) as tar:
                tar.extract(run_file, user_tmp_dir)
        return run_file_path

    def internal_specifics(self):
        """
        Optional placeholder for company-specific implementation.
        For example, you might use this to setup:
        - Team level cost attribution
        - custom json manipulation
        - etc
        """
        pass

    def pre_execute(self, context):
        if self.pre_execute_callback:
            try:
                self.pre_execute_callback(context)
            except Exception as e:
                logging.warning("[Callback] Failed to execute pre_execute_callback")
                logging.exception(e)
        run_file = self.download_chronon_api_package(self.API_VERSION)
        ds_format = "%Y-%m-%d"
        ds = (datetime.strptime(context["ds"], ds_format) - timedelta(days=self.lag)).strftime(ds_format)
        self.bash_command = f"python3 {run_file} {self.runpy_args} --ds {ds}"


class SensorWithEndDate(NamedHivePartitionSensor):
    """
    Sensor that can read an end_date from the parameters and return early if sensing for after the end date.
    We still prefer named hive partition sensors because of smart sensing capabilities and concurrency.
    However whenever a dependency has an end date we need to avoid sensing for it past the end date.
    """
    def execute(self, context):
        if self.params and self.params['end_partition']:
            if self.params['end_partition'] <= context['ds']:
                logging.info(f"Detected end partition {self.params['end_partition']}, dag run date is {context['ds']}. Exiting early.")
                return True
        super(SensorWithEndDate, self).execute(*args, **kwargs)


class PythonSensor(BaseSensorOperator):
    """
    - Change the custom skip logic to look backwards as well as forwards.
    - Looking at tomorrow: If the data has already landed, we would like to skip the skip operator and the spark task it is blocking
    - Looking at yesterday:
       if the spark job is still running, cannot continue, must continue to wait
       if the spark job is in a successful state, we are good to go
       if the spark job has not started or is in some other "unfinished" state, we should only advance if the DAGRun for that day is completed (SUCCESS/FAIL)
    - Looking at tomorrow:
       if the data is already there, we can raise a Skip (which will propagate to spark job)
       if the data is not there, we can run today (so long as yesterday check also passed)
    - Even when data is there for tomorrow (skip case) we cannot skip unless yesterday has been satisfied



    A few followups could be useful:
       (1) Switch task to reschedule once airflow version is updated
       https://airflow.readthedocs.io/en/stable/_api/airflow/sensors/base_sensor_operator/index.html#airflow.sensors.base_sensor_operator.BaseSensorOperator.valid_modes
       (2) Filter tasks to only include the largest attempt - more resilient to clearing old days
       (3) Use built-in python sensor once made available
       https://airflow.apache.org/_api/airflow/contrib/sensors/python_sensor/index.html#airflow.contrib.sensors.python_sensor.PythonSensor
    """

    @apply_defaults
    def __init__(
            self,
            python_callable,
            python_args=None,
            python_kwargs=None,
            provide_context=True,
            *args, **kwargs
    ):
        super(PythonSensor, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.python_args = python_args or []
        self.python_kwargs = python_kwargs or {}
        self.provide_context = provide_context

    def poke(self, context):
        kwargs = self.python_kwargs
        if self.provide_context:
            kwargs = dict(kwargs)
            kwargs.update(context)

        self.log.info("Poking callable")
        return bool(self.python_callable(*self.python_args, **kwargs))


@provide_session
def __custom_skip(session=None, task=None, execution_date=None, backward_days=1, forward_days=1, **other_ignored):
    dag_id = task.dag.dag_id
    upstream_task_ids = {t.task_id for t in task.upstream_list}
    downstream_task_ids = {t.task_id for t in task.downstream_list}
    next_execution_date = task.dag.following_schedule(execution_date)
    prev_execution_date = task.dag.previous_schedule(execution_date)

    yester_check = False
    for _ in range(backward_days):
        yester_check = __check_yesterday(session, prev_execution_date, dag_id, downstream_task_ids)
        if yester_check:
            break
        prev_execution_date = task.dag.previous_schedule(prev_execution_date)

    for _ in range(forward_days):
        tomorrow_check = __check_tomorrow(session, next_execution_date, dag_id, task.task_id, upstream_task_ids)
        logging.info("Yesterday check was {} and tomorrow check for {} was {}"
                     .format(yester_check, next_execution_date, tomorrow_check))
        if yester_check and tomorrow_check:
            logging.info("Skipping")
            raise AirflowSkipException("Tomorrow check failed, day is already ready to run")
        next_execution_date = task.dag.following_schedule(next_execution_date)
    return yester_check


def __check_yesterday(session, prev_execution_date, dag_id, downstream_task_ids):
    """
    For downstream tasks of previous execution:
        - Return False if any is running
        - Return True if all have a success
        - Return True if there is a dag run from yesterday finished
        - Return True for no dag run at all (expected first run case)
    """
    all_tasks = list(
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == prev_execution_date,
            TaskInstance.task_id.in_(downstream_task_ids),
        )
    )
    logging.info("Found {} task instances for {} on {}".format(
        len(all_tasks), downstream_task_ids, prev_execution_date))
    dag_runs = list(session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date == prev_execution_date,
    ))
    logging.info("Found {} dag runs for {}".format(len(dag_runs), prev_execution_date))
    found_dag_finished = False
    found_dag_unfinished = False
    for dag_run in dag_runs:
        logging.info("Dag Run in state {}".format(dag_run.state))
        if dag_run.state in State.finished():
            found_dag_finished = True
        else:
            found_dag_unfinished = True
    finished_task_ids = set()
    for task_inst in all_tasks:
        logging.info("Task Instance ({}) in state {}".format(task_inst.task_id, task_inst.state))
        if task_inst.state == State.RUNNING:
            logging.info("Found running task. Returning False")
            return False
        if task_inst.state not in State.unfinished():
            finished_task_ids.add(task_inst.task_id)
    if len(downstream_task_ids) == len(finished_task_ids):
        logging.info("DagRun not complete, but required tasks are complete; returning True.")
        return True
    return found_dag_finished or not found_dag_unfinished


def __check_tomorrow(session, next_execution_date, dag_id, task_id, upstream_task_ids):
    if upstream_task_ids:
        return __check_tomorrow_non_empty_upstream(session, next_execution_date, dag_id, upstream_task_ids)
    return __check_tomorrow_empty_upstream(session, next_execution_date, dag_id, task_id)


def __check_tomorrow_non_empty_upstream(session, next_execution_date, dag_id, upstream_task_ids):
    """
    Return true iff all upstream tasks (for following execution date)
    have finished successfully
    """
    tasks = {
        ti
        for ti in session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == next_execution_date,
            TaskInstance.task_id.in_(upstream_task_ids),
            TaskInstance.state == State.SUCCESS
        )
    }
    logging.info("Found {} task instances for {} on {}".format(
        len(tasks), upstream_task_ids, next_execution_date))
    success_tasks = {
        ti.task_id for ti in tasks if ti.state == State.SUCCESS
    }
    logging.info("Found {} out of {} unique successful tasks".format(
        len(success_tasks), len(set(upstream_task_ids))
    ))
    return len(success_tasks) == len(set(upstream_task_ids))


def __check_tomorrow_empty_upstream(session, next_execution_date, dag_id, task_id):
    """
    If any instance of tomorrow exists, then tomorrow is ready.
    """
    return bool({
        ti
        for ti in session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == next_execution_date,
            TaskInstance.task_id == task_id
        )
    })


def create_skip_operator(dag, name, poke_interval=None, backward_days=1, forward_days=1):
    return PythonSensor(
        dag=dag,
        task_id='custom_skip__{}'.format(name),
        python_callable=__custom_skip,
        python_kwargs={'forward_days': forward_days, 'backward_days': backward_days},
        poke_interval=poke_interval or 15 * 60,  # 15 minutes. This should be high because it is querying the sql db
        task_concurrency=10  # Setting to 10 to avoid deadlock
    )
