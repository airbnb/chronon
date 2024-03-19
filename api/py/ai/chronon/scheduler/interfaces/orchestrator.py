from abc import ABC, abstractmethod


class WorkflowOrchestrator(ABC):
    @abstractmethod
    def schedule_task(self, task):
        pass

    @abstractmethod
    def set_dependencies(self, task, dependencies):
        pass

    @abstractmethod
    def build_dag_from_flow(self, flow):
        pass
