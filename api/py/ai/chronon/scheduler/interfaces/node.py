class Node:
    def __init__(self, name, command, settings, *args, **kwargs):
        self.name = name
        self.command = command
        self.settings = settings
        self.args = args
        self.kwargs = kwargs
        self.dependencies = set()

    def add_dependency(self, node):
        self.dependencies.add(node)

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.name == other.name
        return False

    def __hash__(self):
        return hash(self.name)
