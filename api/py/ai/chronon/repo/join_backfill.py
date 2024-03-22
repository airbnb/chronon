import logging
from datetime import datetime

import airflow_client
from ai.chronon.join import Join
from ai.chronon.repo.run import download_jar
from ai.chronon.scheduler.adapters.airflow_adapter import AirflowOrchestrator
from ai.chronon.scheduler.interfaces.flow import Flow
from ai.chronon.scheduler.interfaces.node import Node
from ai.chronon.utils import join_part_name

SPARK_VERSION = "3.1.1"
SPARK_JAR_TYPE = "uber"
logging.basicConfig(level=logging.INFO)
airflow_client.init(airflow_client.Service.STONE)


class JoinBackfill:
    def __init__(self, join: Join, start_date: str, end_date: str, config_path: str):
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
        self.config_path = config_path
        self.config_name = config_path.split("/")[-1]

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
        cmd = f"aws s3 cp {self.config_path} . && "
        cmd += (
            "emr-spark-submit"
            + " --spark-version 3.1.1"
            + " --emr-cluster backfill-shared-prod"
            + " --hive-cluster silver"
            + " --queue backfill"
            + " --deploy-mode client"
            + " --master yarn"
            + " --executor-memory 4G"
            + " --driver-memory 4G"
            + " --jars ''"
            + " --class ai.chronon.spark.Driver"
            + f" {self.jar_path} join"
            + f" --selected-join-parts={join_part}"
            + f" --conf-path={self.config_name}"
            + f" --end-date={self.end_date}"
        )
        if self.start_date:
            cmd += f" --start-partition-override={self.start_date}"
        return cmd

    def run_left(self):
        # TODO: integrate with the Spark side change
        return "echo 'Running left table'"

    def run_final_join(self):
        # TODO: integrate with the Spark side change
        return "echo 'Running final join'"

    def run(self):
        orchestrator = AirflowOrchestrator(
            dag_id="join_backfill",
            start_date=datetime.strptime(self.start_date, "%Y-%m-%d"),
        )
        dag = orchestrator.build_dag_from_flow(self.build_flow())
        airflow_client.create_dag(dag, overwrite=True)
