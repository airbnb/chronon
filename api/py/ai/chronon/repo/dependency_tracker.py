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
from ai.chronon.logger import get_logger
from typing import List
from ai.chronon.repo.serializer import \
    file2thrift
from ai.chronon.api.ttypes import \
    GroupBy, Join, Source, Derivation, ExternalPart
from ai.chronon.repo import JOIN_FOLDER_NAME, \
    GROUP_BY_FOLDER_NAME
from ai.chronon.repo.validator import extract_json_confs


class ChrononEntityDependencyTracker(object):
    def __init__(self, chronon_root_path: str, conf_path: str, log_level=logging.INFO):
        self.logger = get_logger(log_level)
        self.chronon_root_path = chronon_root_path
        self.conf_path = conf_path

    def check_join_downstream(self) -> List[str]:
        joins = self.extract_json_confs(
            Join,
            os.path.join(self.chronon_root_path, self.conf_path))
        downstreams = []
        if len(joins) == 0:
            print(f"No joins found in {self.conf_path}")
            return downstreams
        elif len(joins) > 1:
            print(f"Multiple joins found in {self.conf_path}")
            return downstreams
        join = joins[0]
        join_name = join.metaData.name
        group_bys = self.extract_json_confs(
            GroupBy,
            os.path.join(self.chronon_root_path, GROUP_BY_FOLDER_NAME))
        for group_by in group_bys:
            for source in group_by.sources:
                if source.joinSource and source.joinSource.join.metaData.name == join_name:
                    downstreams.append(group_by.metaData.name)
        return downstreams

    def check_group_by_downstream(self) -> List[str]:
        group_bys = self.extract_json_confs(
            GroupBy,
            os.path.join(self.chronon_root_path, self.conf_path))
        downstreams = []
        if len(group_bys) == 0:
            print(f"No group_bys found in {self.conf_path}")
            return downstreams
        elif len(group_bys) > 1:
            print(f"Multiple group_bys found in {self.conf_path}")
            return downstreams
        group_by = group_bys[0]
        group_by_name = group_by.metaData.name
        joins = self.extract_json_confs(
            Join,
            os.path.join(self.chronon_root_path, JOIN_FOLDER_NAME))
        for join in joins:
            for join_part in join.joinParts:
                if join_part.groupBy.metaData.name == group_by_name:
                    downstreams.append(join.metaData.name)
        return downstreams

    def check_downstream(self):
        if "joins" in self.conf_path:
            downstream = self.check_join_downstream()
        elif "group_bys" in self.conf_path:
            downstream = self.check_group_by_downstream()
        else:
            print(f"Invalid conf path: {self.conf_path}")
            downstream = []
        return downstream
