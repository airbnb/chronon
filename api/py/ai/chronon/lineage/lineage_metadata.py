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
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple


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


class Table:
    def __init__(self, table_name: str, table_type: TableType) -> None:
        """
        Instance initializer for the Table class.

        :param table_name: The name of the table.
        :param table_type: The type of the table as defined in TableType.
        """
        self.columns: Set[str] = set()  # Set to hold column names for this table.
        self.key_columns: Set[str] = set()  # Set to hold key column names.
        self.table_name: str = table_name
        self.table_type: TableType = table_type


class Feature:
    def __init__(self, entity_name: str, feature_name: str, column: Optional[str] = None) -> None:
        """
        Instance initializer for the Feature class.

        :param entity_name: Name of the entity.
        :param feature_name: Name of the feature.
        :param column: Optional fully qualified column name if available.
        """
        self.entity_name: str = entity_name
        self.feature_name: str = feature_name
        self.column: Optional[str] = column


class LineageMetaData:
    def __init__(self) -> None:
        """
        Initializes a new instance of LineageMetaData for storing data lineage information.

        This class maintains:
          - A set of lineage tuples: (input_column, output_column, combined_operations).
          - A mapping of table names to Table objects.
          - A mapping of feature identifiers to Feature objects.
          - A mapping of tables to lists of unparsed column names.
        """
        self.lineages: Set[Tuple[str, str, str]] = set()
        self.tables: Dict[str, Table] = {}
        self.features: Dict[str, Feature] = {}
        self.unparsed_columns: Dict[str, List[str]] = defaultdict(list)

    def store_column(self, column: str) -> None:
        """
        Instance method to extract and store a column into its corresponding Table.

        The method splits a fully qualified column name (e.g., "schema.table.column")
        to determine the table name and column name. If the table does not already exist,
        it is created with a default type of OTHER.

        :param column: Fully qualified column name.
        """
        # Split the string to get table and column parts.
        parts = column.split(".")
        table_name = ".".join(parts[:-1])
        column_name = parts[-1]

        # Create a new Table if one does not exist for the extracted table name.
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name, TableType.OTHER)

        # Add the column name to the table's set of columns.
        self.tables[table_name].columns.add(column_name)

    def store_feature(self, entity_name: str, feature_name: str, output_table: Optional[str] = None) -> None:
        """
        Instance method to create and store a Feature.

        Builds a feature identifier by combining the entity and feature names.
        If an output table is provided, it constructs a fully qualified column name.

        :param entity_name: Name of the entity.
        :param feature_name: Name of the feature.
        :param output_table: Optional table name where the feature is stored.
        """
        feature_identifier = f"{entity_name}.{feature_name}"
        if output_table:
            # If an output table is provided, construct the full column name.
            column = f"{output_table}.{feature_identifier}"
            self.features[feature_identifier] = Feature(entity_name, feature_identifier, column)
        else:
            self.features[feature_identifier] = Feature(entity_name, feature_identifier)

    def store_table(self, table_name: str, table_type: TableType) -> None:
        """
        Instance method to create and store a Table if it does not already exist.

        :param table_name: Name of the table.
        :param table_type: The type of the table as defined in TableType.
        """
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name, table_type)

    def store_group_by_feature(self, entity_name: str, feature_name: str) -> None:
        """
        Instance method to store a group-by feature without linking it to an output table.

        :param entity_name: Name of the entity.
        :param feature_name: Name of the feature.
        """
        feature_identifier = f"{entity_name}.{feature_name}"
        self.features[feature_identifier] = Feature(entity_name, feature_identifier)

    def store_lineage(self, parsed_lineages: Dict[str, List[Tuple[str, List[str]]]], table_name: str) -> None:
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
        for output_column, input_columns in parsed_lineages.items():
            # Store the output column in its table.
            self.store_column(output_column)

            # If there are no input columns, record the output column as unparsed.
            if not input_columns:
                self.unparsed_columns[table_name].append(output_column)
                continue

            # Process each input column along with its operations.
            for input_column, operations in input_columns:
                self.store_column(input_column)
                combined_ops = combine_operations(operations)
                self.lineages.add((input_column, output_column, combined_ops))

    def filter_lineages(
        self, input_table: Optional[str] = None, output_table: Optional[str] = None
    ) -> Set[Tuple[str, str, str]]:
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
            filtered_lineages = {lineage for lineage in filtered_lineages if table_name(lineage[0]) == input_table}

        # Filter by output table if provided.
        if output_table:
            filtered_lineages = {lineage for lineage in filtered_lineages if table_name(lineage[1]) == output_table}

        return filtered_lineages


def combine_operations(operations: List[str]) -> str:
    """
    Utility function to combine a list of operations into a single comma-separated string.

    It excludes any operation named "Alias" and reverses the list to preserve the intended order.

    :param operations: List of operation names.
    :return: A comma-separated string of the filtered and reversed operations.
    """
    # Exclude operations that are "Alias".
    filtered_operations = [op for op in operations if op != "Alias"]
    # Reverse the order of operations to maintain the proper sequence.
    filtered_operations.reverse()
    return ",".join(filtered_operations)


def table_name(column_name: str) -> str:
    """
    Utility function to extract the table name from a fully qualified column name.

    It does so by removing the last segment (assumed to be the column name) from the string.

    :param column_name: Fully qualified column name (e.g., "db.table.column").
    :return: Extracted table name (e.g., "db.table").
    """
    parts = column_name.split(".")
    return ".".join(parts[:-1])
