import logging
from collections import defaultdict
from enum import Enum
from typing import Dict, List, Set, Tuple

logger = logging.getLogger(__name__)


class TableType(str, Enum):
    GROUP_BY_UPLOAD = "group_by_upload"
    GROUP_BY_BACKFILL = "group_by_backfill"
    JOIN_BOOTSTRAP = "join_bootstrap"
    JOIN_PART = "join_part"
    JOIN = "join"
    STAGING_QUERY = "staging_query"
    OTHER = "other"


class Table:
    def __init__(self, table_name, table_type) -> None:
        self.columns = set()  # Set to hold column names for this table.
        self.table_name = table_name
        self.table_type = table_type


class Feature:
    def __init__(self, entity_name, feature_name, column=None) -> None:
        self.entity_name = entity_name
        self.feature_name = feature_name
        self.column = column


class LineageMetaData:
    def __init__(self) -> None:
        # Set of tuples containing (input_column, output_column, operation)
        self.lineages: Set[Tuple[str, str, str]] = set()
        # Dictionary mapping table name to Table objects.
        self.tables: Dict[str, Table] = dict()
        # Dictionary mapping feature identifier to Feature objects.
        self.features: Dict[str, Feature] = dict()
        # Dictionary to hold columns that couldn't be parsed, grouped by table.
        self.unparsed_columns: Dict[str, List[str]] = defaultdict(list)

    def store_column(self, column):
        """
        Extract the table name and column name from a fully qualified column name
        and store the column in the corresponding Table.

        :param column: Fully qualified column name (e.g., "schema.table.column").
        """
        # Split the column string to separate the table and column parts.
        table_name = ".".join(column.split(".")[:-1])
        column_name = column.split(".")[-1]
        # Create a new table if it doesn't exist yet.
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name, TableType.OTHER)
        # Add the column name to the table's columns set.
        self.tables[table_name].columns.add(column_name)

    def store_feature(self, entity_name, feature_name, output_table):
        """
        Create and store a Feature object based on the entity and feature names.
        If an output table is provided, the feature's column is constructed accordingly.

        :param entity_name: Name of the entity.
        :param feature_name: Name of the feature.
        :param output_table: Table name where the feature is stored.
        """
        # Construct the feature identifier.
        feature_identifier = f"{entity_name}.{feature_name}"
        if output_table:
            # If output_table is given, create a full column name.
            column = f"{output_table}.{feature_identifier}"
            self.features[feature_identifier] = Feature(entity_name, feature_identifier, column)
        else:
            self.features[feature_identifier] = Feature(entity_name, feature_identifier)

    def store_table(self, table_name, table_type):
        """
        Create and store a Table object if it does not already exist.

        :param table_name: Name of the table.
        :param table_type: Type of the table as defined in TableType.
        """
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name, table_type)

    def store_group_by_feature(self, entity_name, feature_name):
        """
        Store a group-by feature without associating it with an output table.

        :param entity_name: Name of the entity.
        :param feature_name: Name of the feature.
        """
        feature_identifier = f"{entity_name}.{feature_name}"
        self.features[feature_identifier] = Feature(entity_name, feature_identifier)

    def store_lineage(self, parsed_lineages, table_name):
        """
        Store lineage information by mapping input columns to output columns along with operations.
        Unparsed output columns (with no input columns) are recorded separately.

        :param parsed_lineages: Dictionary with output columns as keys and lists of tuples
                                (input_column, operation) as values.
        :param table_name: The table name to associate with unparsed columns.
        """
        for output_column, input_columns in parsed_lineages.items():
            # Store the output column in the corresponding table.
            self.store_column(output_column)
            # If there are no input columns, mark the column as unparsed.
            if not input_columns:
                self.unparsed_columns[table_name].append(output_column)
                continue
            # Process each input column and its associated operations.
            for input_column, operation in input_columns:
                self.store_column(input_column)
                # Combine operations (excluding "Alias") and add the lineage tuple.
                self.lineages.add((input_column, output_column, combine_operations(operation)))

    def filter_lineages(self, input_table=None, output_table=None):
        """
        Filter stored lineages based on input and/or output table names.

        :param input_table: (Optional) Filter lineages to include only those whose input column belongs to this table.
        :param output_table: (Optional) Filter lineages to include only those whose output column belongs to this table.
        :return: A set of lineage tuples that match the filter criteria.
        """
        lineages = self.lineages
        # Filter by input table if specified.
        if input_table:
            lineages = {lineage for lineage in lineages if table_name(lineage[0]) == input_table}
        # Filter by output table if specified.
        if output_table:
            lineages = {lineage for lineage in lineages if table_name(lineage[1]) == output_table}
        return lineages


def combine_operations(operations):
    """
    Combine a list of operations into a single comma-separated string,
    excluding any 'Alias' operations.

    :param operations: List of operation names.
    :return: A comma-separated string of operations.
    """
    # Remove operations that are 'Alias'.
    filtered_operations = [operation for operation in operations if operation != "Alias"]
    # Reverse the list to preserve the correct order.
    filtered_operations.reverse()
    return ",".join(filtered_operations)


def table_name(column_name):
    """
    Extract the table name from a fully qualified db.table.column name by removing the last column name.
    """
    return ".".join(column_name.split(".")[:-1])
