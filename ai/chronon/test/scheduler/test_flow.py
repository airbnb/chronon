import unittest

from ai.chronon.scheduler.interfaces.flow import Flow
from ai.chronon.scheduler.interfaces.node import Node


class TestFlow(unittest.TestCase):
    def setUp(self):
        self.flow = Flow("test_flow")
        self.node1 = Node("node1", "command1")
        self.node2 = Node("node2", "command2")

    def test_add_node(self):
        self.flow.add_node(self.node1)
        self.assertIn(self.node1, self.flow.nodes)

    def test_find_node(self):
        self.flow.add_node(self.node1)
        self.assertEqual(self.flow.find_node("node1"), self.node1)
        self.assertIsNone(self.flow.find_node("node2"))


if __name__ == "__main__":
    unittest.main()
