"""Object for checking the downstream dependency of an entity in the Chronon repository.
"""

#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
import logging
import os
from typing import List

from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.logger import get_logger
from ai.chronon.repo import GROUP_BY_FOLDER_NAME, JOIN_FOLDER_NAME
from ai.chronon.repo.validator import extract_json_confs


class ChrononEntityDependencyTracker(object):
    def __init__(self, chronon_root_path: str, log_level=logging.INFO):
        self.logger = get_logger(log_level)
        self.chronon_root_path = chronon_root_path

    def extract_conf(self, obj_class: type, path: str) -> object:
        obj = extract_json_confs(obj_class, os.path.join(self.chronon_root_path, path))
        if len(obj) == 0:
            raise Exception(f"No {obj_class} found in {path}")
        elif len(obj) > 1:
            raise Exception(f"Multiple {obj_class} found in {path}")
        return obj[0]

    def get_join_downstream(self, conf_path: str) -> List[object]:
        join = self.extract_conf(Join, conf_path)
        join_name = join.metaData.name
        names_set = set()
        downstream = list()
        group_bys = extract_json_confs(GroupBy, os.path.join(self.chronon_root_path, GROUP_BY_FOLDER_NAME))
        for group_by in group_bys:
            for source in group_by.sources:
                if (
                    source.joinSource
                    and source.joinSource.join.metaData.name == join_name
                    and group_by.metaData.name not in names_set
                ):
                    names_set.add(group_by.metaData.name)
                    downstream.append(group_by)
        return downstream

    def get_group_by_downstream(self, conf_path: str) -> List[object]:
        group_by = self.extract_conf(GroupBy, conf_path)
        group_by_name = group_by.metaData.name
        names_set = set()
        downstream = list()
        joins = extract_json_confs(Join, os.path.join(self.chronon_root_path, JOIN_FOLDER_NAME))
        for join in joins:
            for join_part in join.joinParts:
                if join_part.groupBy.metaData.name == group_by_name and join.metaData.name not in names_set:
                    names_set.add(join.metaData.name)
                    downstream.append(join)
        return downstream

    def get_downstream(self, conf_path: str) -> List[object]:
        if "joins" in conf_path:
            downstream = self.get_join_downstream(conf_path)
        elif "group_bys" in conf_path:
            downstream = self.get_group_by_downstream(conf_path)
        # TODO: Implement the downstream check for staging queries
        elif "staging_queries" in conf_path:
            self.logger.info("Staging queries downstream check not supported yet.")
            downstream = set()
        else:
            raise Exception(f"Invalid conf path: {conf_path}")
        return downstream

    def get_downstream_names(self, conf_path: str) -> List[str]:
        downstream = self.get_downstream(conf_path)
        downstream_name = list()
        for obj in downstream:
            downstream_name.append(obj.metaData.name)
        return downstream_name
