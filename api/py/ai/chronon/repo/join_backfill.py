import logging
import os
from datetime import datetime

import airflow_client
from ai.chronon.join import Join
from ai.chronon.repo.run import download_jar
from ai.chronon.scheduler.adapters.airflow_adapter import AirflowOrchestrator
from ai.chronon.scheduler.interfaces.flow import Flow
from ai.chronon.scheduler.interfaces.node import Node
from ai.chronon.utils import join_part_name, sanitize

SPARK_VERSION = "3.1.1"
SPARK_JAR_TYPE = "uber"
EXECUTOR_MEMORY = "4g"
DRIVER_MEMORY = "4g"
AIRFLOW_CLUSTER = airflow_client.Service.STONE
logging.basicConfig(level=logging.INFO)


class JoinBackfill:
    def __init__(
        self,
        join: Join,
        start_date: str,
        end_date: str,
        config_path: str,
        s3_bucket: str,
        spark_version: str = SPARK_VERSION,
        airflow_cluster: str = AIRFLOW_CLUSTER,
        executor_memory: str = EXECUTOR_MEMORY,
        driver_memory: str = DRIVER_MEMORY,
    ):
        self.dag_id = "_".join(
            map(sanitize, ["chronon_join_backfill", os.path.basename(config_path).split("/")[-1], start_date, end_date])
        )
        self.join = join
        self.start_date = start_date
        self.end_date = end_date
        self.jar_path = download_jar(
            "latest",
            jar_type=SPARK_JAR_TYPE,
            release_tag=None,
            spark_version=SPARK_VERSION,
            skip_download=True,
        )
        self.s3_bucket = s3_bucket
        self.config_path = config_path
        self.spark_version = spark_version
        self.airflow_cluster = airflow_cluster
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory

    def build_flow(self) -> Flow:
        """
        Build a flow from a Join object. Each join part is a node and will run in parallel.
        The next step is final join, which is a node that depends on all join parts.
        The final join will run after all join parts are done.

        :param join: The Join object to build a flow from
        :return: A Flow object that represents the flow of the Join
        """
        flow = Flow(self.join.metaData.name)
        final_node = Node("final_join", self.run_final_join())
        left_node = Node("left_table", self.run_left())
        flow.add_node(final_node)
        flow.add_node(left_node)
        for join_part in self.join.joinParts:
            jp_full_name = join_part_name(join_part)
            jp_node = Node(jp_full_name, self.run_join_part(jp_full_name))
            flow.add_node(jp_node)
            jp_node.add_dependency(left_node)
            final_node.add_dependency(jp_node)
        return flow

    def run_join_part(self, join_part: str):
        # TODO: Find a better way to sync configs
        config_dir = os.path.dirname(self.config_path) + "/"
        cmd = f"""
        aws s3 cp {self.s3_bucket}{self.config_path} /tmp/{config_dir} &&
        aws s3 cp {self.s3_bucket}run.py /tmp/ &&
        aws s3 cp {self.s3_bucket}spark_submit.sh /tmp/ &&
        export SPARK_VERSION={self.spark_version} &&
        export EXECUTOR_MEMORY={self.executor_memory} &&
        export DRIVER_MEMORY={self.driver_memory} &&
        python3 /tmp/run.py --mode=backfill --conf=/tmp/{self.config_path} --env=production --spark-submit-path /tmp/spark_submit.sh --selected-join-parts={join_part} --ds={self.end_date}"""
        if self.start_date:
            cmd += f" --start-ds={self.start_date}"
        return cmd

    def run_left(self):
        # TODO: integrate with the Spark side change
        return "echo 'Running left table'"

    def run_final_join(self):
        # TODO: integrate with the Spark side change
        return "echo 'Running final join'"

    def run(self):
        airflow_client.init(self.airflow_cluster)
        orchestrator = AirflowOrchestrator(
            dag_id=self.dag_id,
            start_date=datetime.strptime(self.start_date, "%Y-%m-%d"),
        )
        dag = orchestrator.build_dag_from_flow(self.build_flow())
        airflow_client.create_dag(dag, overwrite=True)
