import json
import os
from typing import Optional

from ai.chronon.scheduler.interfaces.flow import Flow
from ai.chronon.scheduler.interfaces.node import Node
from ai.chronon.utils import (
    convert_json_to_obj,
    dict_to_bash_commands,
    dict_to_exports,
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
        settings: dict = DEFAULT_SPARK_SETTINGS,
    ):
        self.dag_id = "_".join(
            map(
                sanitize, ["chronon_joins_backfill", os.path.basename(config_path).split("/")[-1], start_date, end_date]
            )
        )
        self.start_date = start_date
        self.end_date = end_date
        self.config_path = config_path
        self.settings = settings
        with open(self.config_path, "r") as file:
            config = file.read()
        self.join = convert_json_to_obj(json.loads(config))

    def build_flow(self) -> Flow:
        """
        Build a flow from a Join object. Each join part is a node and will run in parallel.
        The next step is final join, which is a node that depends on all join parts.
        The final join will run after all join parts are done.

        :param join: The Join object to build a flow from
        :return: A Flow object that represents the flow of the Join
        """
        flow = Flow(self.join.metaData.name)
        final_node = Node(
            f"{TASK_PREFIX}__{sanitize(get_join_output_table_name(self.join, full_name=True))}", self.run_final_join()
        )
        left_node = Node(f"{TASK_PREFIX}__left_table", self.run_left_table())
        flow.add_node(final_node)
        flow.add_node(left_node)
        for join_part in self.join.joinParts:
            jp_full_name = join_part_name(join_part)
            jp_node = Node(f"{TASK_PREFIX}__{jp_full_name}", self.run_join_part(jp_full_name))
            flow.add_node(jp_node)
            jp_node.add_dependency(left_node)
            final_node.add_dependency(jp_node)
        return flow

    def export_template(self, settings: dict):
        return f"{dict_to_exports(settings)}"

    def command_template(self, extra_args: dict):
        if self.start_date:
            extra_args.update({"start_ds": self.start_date})
        return f"""python3 /tmp/run.py --conf=/tmp/{self.config_path} --ds={self.end_date} \
{dict_to_bash_commands(extra_args)}"""

    def run_join_part(self, join_part: str):
        args = {
            "mode": "backfill",
            "selected_join_parts": join_part,
            "use_cached_left": None,
        }
        settings = self.settings.get(join_part, self.settings["default"])
        return self.export_template(settings) + " && " + self.command_template(extra_args=args)

    def run_left_table(self):
        settings = self.settings.get("left_table", self.settings["default"])
        return self.export_template(settings) + " && " + self.command_template(extra_args={"mode": "backfill-left"})

    def run_final_join(self):
        settings = self.settings.get("final_join", self.settings["default"])
        return self.export_template(settings) + " && " + self.command_template(extra_args={"mode": "backfill-final"})

    def run(self, orchestrator: str, overrides: Optional[dict] = None):
        from ai.chronon.constants import ADAPTERS

        ADAPTERS.update(overrides)
        orchestrator = ADAPTERS[orchestrator](dag_id=self.dag_id, start_date=self.start_date)
        orchestrator.setup()
        orchestrator.build_dag_from_flow(self.build_flow())
        orchestrator.trigger_run()
