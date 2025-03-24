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

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class TableType(str, Enum):
    """
    Enum representing the various types of tables.
    """

    GROUP_BY_UPLOAD = "group_by_upload"
    GROUP_BY_BACKFILL = "group_by_backfill"
    JOIN_BOOTSTRAP = "join_bootstrap"
    JOIN_PART = "join_part"
    JOIN = "join"
    STAGING_QUERY = "staging_query"
    OTHER = "other"


@dataclass
class Table:
    config_name: str
    table_name: str
    table_type: TableType
    materialized: bool
    key_columns: Set[str] = None
    columns: Set[str] = field(default_factory=set)


class ConfigType(str, Enum):
    """
    Enum representing the various types of config.
    """

    GROUP_BY = "group_by"
    JOIN = "join"
    STAGING_QUERY = "staging_query"


@dataclass(frozen=True)
class Feature:
    config_name: str
    config_type: ConfigType
    feature_name: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None


@dataclass(frozen=True)
class ColumnTransform:
    input_table: str
    input_column: str
    output_table: str
    output_column: str
    transforms: Tuple[str]


@dataclass
class Config:
    config_name: str
    config_type: ConfigType
    t_object: Any
    tables: Dict[str, Table] = field(default_factory=defaultdict)
    features: Dict[str, Feature] = field(default_factory=defaultdict)


class LineageMetaData:
    def __init__(self) -> None:
        """
        Initializes a new instance of LineageMetaData for storing data lineage information.

        """
        self.lineages: Set[ColumnTransform] = set()
        self.tables: Dict[str, Table] = {}
        self.features: Dict[str, Feature] = {}
        self.configs: Dict[str, Config] = {}

        self.unparsed_configs: Dict[str, List[str]] = defaultdict(list)
        self.unparsed_columns: Dict[str, List[str]] = defaultdict(list)

    def store_column(self, table_name: str, column_name: str) -> None:
        """
        Instance method to extract and store a column into its corresponding Table.

        :param table_name: Table name.
        :param column_name: Column name.
        """
        # Create a new Table if one does not exist for the extracted table name.
        if table_name not in self.tables:
            self.tables[table_name] = Table("", table_name, TableType.OTHER, materialized=True)

        # Add the column name to the table's set of columns.
        self.tables[table_name].columns.add(column_name)

    def store_feature(
        self,
        config_name: str,
        config_type: ConfigType,
        feature_name: str,
        output_table: Optional[str] = None,
    ) -> None:
        """
        Instance method to create and store a Feature.

        Builds a feature identifier by combining the entity and feature names.
        If an output table is provided, it constructs a fully qualified column name.

        :param config_name: Name of the config.
        :param config_type: Type of the config - GroupBy or Join.
        :param feature_name: Name of the feature.
        :param output_table: Optional table name where the feature is stored.
        """
        feature_full_name = f"{config_name}.{feature_name}"
        if output_table:
            self.features[feature_full_name] = Feature(
                config_name, config_type, feature_full_name, output_table, feature_name
            )
        else:
            self.features[feature_full_name] = Feature(config_name, config_type, feature_full_name)

        self.configs[config_name].features[feature_full_name] = self.features[feature_full_name]

    def store_table(
        self,
        config_name: str,
        table_name: str,
        table_type: TableType,
        materialized: bool = True,
        key_columns: Set[str] = None,
    ) -> None:
        """
        Instance method to create and store a Table if it does not already exist.

        :param config_name: Name of the config.
        :param table_name: Name of the table.
        :param table_type: The type of the table as defined in TableType.
        :param materialized: If the table is materialized.
        :param key_columns: The key columns used to join with other tables.
        """
        if table_name not in self.tables:
            self.tables[table_name] = Table(config_name, table_name, table_type, materialized, key_columns)
        else:
            # replace existing table with new parsed table type
            existing_table = self.tables[table_name]
            if existing_table.table_type == TableType.OTHER and table_type != TableType.OTHER:
                self.tables[table_name].table_type = table_type
                self.tables[table_name].config_name = config_name
                self.tables[table_name].key_columns = key_columns

        self.configs[config_name].tables[table_name] = self.tables[table_name]

    def store_lineage(
        self,
        parsed_lineages: Dict[Tuple[str, str], Set[Tuple[Tuple[str, str], Tuple[str]]]],
        table_name: str,
    ) -> None:
        """
        Instance method to record lineage information by mapping input columns to output columns with operations.

        For each output column:
          - It stores the output column.
          - If there are no input columns, the output column is recorded as unparsed.
          - Otherwise, for each input column, it stores the input column and records the lineage
            tuple, combining operations (ignoring "Alias" operations).

        :param parsed_lineages: Dictionary mapping output columns to lists of tuples (input_column, operations).
        :param table_name: Table name to associate unparsed columns.
        """
        for (
            output_table,
            output_column,
        ), input_column_transforms in parsed_lineages.items():
            # Store the output column in its table.
            self.store_column(output_table, output_column)

            # If there are no input columns, record the output column as unparsed.
            if not input_column_transforms:
                self.unparsed_columns[table_name].append(output_column)
                continue

            # Process each input column along with its operations.
            for (input_table, input_column), transforms in input_column_transforms:
                self.store_column(input_table, input_column)
                self.lineages.add(
                    ColumnTransform(
                        input_table,
                        input_column,
                        output_table,
                        output_column,
                        # Reverse transform from input_column to output_column
                        transforms[::-1],
                    )
                )

    def filter_lineages(
        self, input_table: Optional[str] = None, output_table: Optional[str] = None
    ) -> Set[ColumnTransform]:
        """
        Instance method to filter stored lineage tuples based on table names.

        Filters the lineage data to return only those tuples where the input column's table
        matches input_table and/or the output column's table matches output_table.

        :param input_table: Optional table name for filtering by input column.
        :param output_table: Optional table name for filtering by output column.
        :return: A set of lineage tuples that match the filter criteria.
        """
        filtered_lineages = self.lineages

        # Filter by input table if provided.
        if input_table:
            filtered_lineages = {lineage for lineage in filtered_lineages if lineage.input_table == input_table}

        # Filter by output table if provided.
        if output_table:
            filtered_lineages = {lineage for lineage in filtered_lineages if lineage.output_table == output_table}

        return filtered_lineages


def extract_table_name(full_column_name: str) -> str:
    """
    Utility function to extract the table name from a fully qualified column name.

    :param full_column_name: Fully qualified column name (e.g., "db.table.column").
    :return: Extracted table name (e.g., "db.table").
    """
    parts = full_column_name.split("./")[0].split(".")
    return ".".join(parts[:-1])


def extract_column_name(full_column_name: str) -> str:
    """
    Utility function to extract the column name from a fully qualified column name.

    :param full_column_name: Fully qualified column name (e.g., "db.table.column").
    :return: Extracted column name (e.g., "column").
    """
    table_name = extract_table_name(full_column_name)
    return full_column_name[len(table_name) + 1 :]
