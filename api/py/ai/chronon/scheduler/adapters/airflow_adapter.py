from datetime import datetime

from ai.chronon.scheduler.interfaces.orchestrator import WorkflowOrchestrator

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


class AirflowAdapter(WorkflowOrchestrator):
    def __init__(self, dag_id, start_date, schedule_interval="@once", airflow_cluster=None):
        self.dag = DAG(
            dag_id,
            start_date=datetime.strptime(start_date, "%Y-%m-%d"),
            schedule_interval=schedule_interval,
        )
        self.airflow_cluster = airflow_cluster

    def setup(self):
        """Initialize a connection to Airflow"""

    def schedule_task(self, node):
        return BashOperator(task_id=node.name, dag=self.dag, bash_command=node.command)

    def set_dependencies(self, task, dependencies):
        task.set_upstream(dependencies)

    def build_dag_from_flow(self, flow):
        node_to_task = {node.name: self.schedule_task(node) for node in flow.nodes}
        for node in flow.nodes:
            task = node_to_task[node.name]
            for dep in node.dependencies:
                dep_task = node_to_task[dep.name]
                self.set_dependencies(task, dep_task)
        return self.dag

    def trigger_run(self):
        """Trigger the DAG run"""
