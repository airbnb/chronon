class Flow:
    def __init__(self, name):
        self.name = name
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def find_node(self, name):
        for node in self.nodes:
            if node.name == name:
                return node
        return None

    def visualize(self, node=None, level=0):
        if node is None:
            starts = [
                n
                for n in self.nodes
                if not any(n in node.dependencies for node in self.nodes)
            ]
            for start in starts:
                self.visualize(start, 0)
            return

        print("    " * level + f"- {node.name}")
        for dependency in node.dependencies:
            self.visualize(dependency, level + 1)
