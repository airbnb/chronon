import json
import os

from ai.chronon.scheduler.interfaces.flow import Flow
from ai.chronon.scheduler.interfaces.node import Node
from ai.chronon.scheduler.interfaces.orchestrator import WorkflowOrchestrator
from ai.chronon.utils import (
    convert_json_to_obj,
    dict_to_bash_commands,
    dict_to_exports,
    get_config_path,
    get_join_output_table_name,
    join_part_name,
    sanitize,
)

TASK_PREFIX = "compute_join"
DEFAULT_SPARK_SETTINGS = {
    "default": {
        "spark_version": "3.1.1",
        "executor_memory": "4G",
        "driver_memory": "4G",
        "executor_cores": 2,
    }
}


class JoinBackfill:
    def __init__(
        self,
        start_date: str,
        end_date: str,
        config_path: str,
        extra_args: dict = None,
        settings: dict = None,
    ):
        self.dag_id = "_".join(
            map(
                sanitize, ["chronon_joins_backfill", os.path.basename(config_path).split("/")[-1], start_date, end_date]
            )
        )
        self.start_date = start_date
        self.end_date = end_date
        self.config_path = config_path
        self.extra_args = extra_args or {}
        self.settings = settings or DEFAULT_SPARK_SETTINGS
        with open(self.config_path, "r") as file:
            config = file.read()
        self.join = convert_json_to_obj(json.loads(config))

    def build_flow(self) -> Flow:
        flow = Flow(self.join.metaData.name)
        self.add_join_to_flow(flow, self.join)
        return flow

    def add_join_to_flow(self, flow: Flow, join: dict) -> Node:
        """
        Parse a Join object and add it to the Flow.
        Each join part is a node and will run in parallel. Besides, left table and final join are also nodes.
        We will add Join recursively when we encounter a join source.
        The final join node will be returned to be used as a dependency.
        """
        join_name = sanitize(join.metaData.name)
        # Merge default settings and final_join settings
        settings = {**self.settings["default"], **self.settings.get("final_join", {})}
        final_node = Node(
            f"{TASK_PREFIX}__{sanitize(get_join_output_table_name(join, full_name=True))}",
            self.run_final_join(join.metaData.name, settings),
            settings,
        )
        # Merge default settings and left_table settings
        settings = {**self.settings["default"], **self.settings.get("left_table", {})}
        left_node = Node(
            f"{TASK_PREFIX}__{join_name}__left_table", self.run_left_table(join.metaData.name, settings), settings
        )
        flow.add_node(final_node)
        flow.add_node(left_node)
        for join_part in join.joinParts:
            for src in join_part.groupBy.sources:
                if "joinSource" in src:
                    dep_node = self.add_join_to_flow(flow, src.joinSource.join)
                    left_node.add_dependency(dep_node)
            jp_full_name = join_part_name(join_part)
            # Merge default settings and join_part settings
            settings = {**self.settings["default"], **self.settings.get(jp_full_name, {})}
            jp_node = Node(
                f"{TASK_PREFIX}__{join_name}__{jp_full_name}",
                self.run_join_part(join.metaData.name, jp_full_name, settings),
                settings,
            )
            flow.add_node(jp_node)
            jp_node.add_dependency(left_node)
            final_node.add_dependency(jp_node)
        return final_node

    def export_template(self, settings: dict):
        return f"{dict_to_exports(settings)}"

    def command_template(self, config_path: str, extra_args: dict):
        extra_args.update(self.extra_args)
        if self.start_date:
            extra_args.update({"start_ds": self.start_date})
        return f"""run.py --conf={config_path} --ds={self.end_date} \
{dict_to_bash_commands(extra_args)}"""

    def run_join_part(self, join_name: dict, join_part: str, settings: dict):
        args = {
            "mode": "backfill",
            "selected_join_parts": join_part,
            "use_cached_left": None,
        }
        return (
            self.export_template(settings)
            + " && "
            + self.command_template(config_path=get_config_path(join_name), extra_args=args)
        )

    def run_left_table(self, join_name: str, settings: dict):
        return (
            self.export_template(settings)
            + " && "
            + self.command_template(config_path=get_config_path(join_name), extra_args={"mode": "backfill-left"})
        )

    def run_final_join(self, join_name: str, settings: dict):
        return (
            self.export_template(settings)
            + " && "
            + self.command_template(config_path=get_config_path(join_name), extra_args={"mode": "backfill-final"})
        )

    def run(self, orchestrator: WorkflowOrchestrator):
        orchestrator.setup()
        orchestrator.build_dag_from_flow(self.build_flow())
        orchestrator.trigger_run()
