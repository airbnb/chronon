class Node:
    def __init__(self, name, command, *args, **kwargs):
        self.name = name
        self.command = command
        self.args = args
        self.kwargs = kwargs
        self.dependencies = []

    def add_dependency(self, node):
        self.dependencies.append(node)
