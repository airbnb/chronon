import unittest
from datetime import datetime

from ai.chronon.scheduler.adapters.airflow_adapter import AirflowOrchestrator
from ai.chronon.scheduler.interfaces.flow import Flow
from ai.chronon.scheduler.interfaces.node import Node


class TestAirflowOrchestrator(unittest.TestCase):
    def setUp(self):
        self.dag_id = "test_dag"
        self.start_date = datetime(2024, 1, 1)
        self.airflow_orchestrator = AirflowOrchestrator(self.dag_id, self.start_date)
        self.node = Node("node", 'echo "This is a test!"')
        self.node2 = Node("node2", 'echo "This is a test!"')
        self.flow = Flow("test_flow")
        self.flow.add_node(self.node)
        self.flow.add_node(self.node2)
        self.node.add_dependency(self.node2)

    def test_schedule_task(self):
        task = self.airflow_orchestrator.schedule_task(self.node)
        self.assertEqual(task.task_id, self.node.name)
        self.assertEqual(task.bash_command, self.node.command)

    def test_build_dag_from_flow(self):
        dag = self.airflow_orchestrator.build_dag_from_flow(self.flow)
        self.assertEqual(dag.dag_id, self.dag_id)
        self.assertListEqual(
            [task.task_id for task in dag.tasks], [self.node.name, self.node2.name]
        )
        self.assertListEqual(
            [task.bash_command for task in dag.tasks],
            [self.node.command, self.node2.command],
        )


if __name__ == "__main__":
    unittest.main()
