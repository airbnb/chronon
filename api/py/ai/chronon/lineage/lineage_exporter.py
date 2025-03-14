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

import argparse
import json
import logging
import os
from typing import List, Tuple

from ai.chronon.lineage.lineage_parser import LineageParser

logger = logging.getLogger(__name__)


def _dump_to_file(metadata_to_dump: List[Tuple], file_name: str) -> None:
    """
    Dump metadata as a JSON file.

    :param metadata_to_dump: List of tuples containing metadata information.
    :param file_name: Path where the JSON file will be saved.
    """
    logger.info(f"Dump to file {file_name} ...")
    with open(file_name, "w") as json_file:
        json.dump(metadata_to_dump, json_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lineage Exporter")
    parser.add_argument(
        "--repo",
        help="Repository root path",
    )
    parser.add_argument(
        "--output-root-path",
        help="Lineage output path",
    )
    args, _ = parser.parse_known_args()
    repo = args.repo
    output_root_path = args.output_root_path
    logger.info(f"Repo path: {repo}, output root path: {output_root_path}")
    parser = LineageParser()
    metadata = parser.parse_lineage(repo)

    lineages = [(ct.input_column, ct.output_column, ct.transforms) for ct in metadata.lineages]
    _dump_to_file(lineages, os.path.join(output_root_path, "lineages.json"))

    features = [(f.config_name, f.feature_name, f.column) for f in metadata.features.values()]
    _dump_to_file(features, os.path.join(output_root_path, "features.json"))

    tables = [
        (t.config_name, t.table_name, t.table_type.value, tuple(t.key_columns or ""), tuple(t.columns or ""))
        for t in metadata.tables.values()
    ]
    _dump_to_file(tables, os.path.join(output_root_path, "tables.json"))

    configs = [
        (c.config_name, c.config_type.value, tuple(c.tables.keys()), tuple(c.features.keys()))
        for c in metadata.configs.values()
    ]
    _dump_to_file(configs, os.path.join(output_root_path, "configs.json"))
