## Chronon Lineage Parser

### Overview

Lineage parser is used to extract lineage metadata from Chronon configs.

Although Chronon enforces strict configurations to define which columns are used to compute a feature, tracing column-to-column transformation lineage remains challenging. This is due to the flexibility users have to write arbitrary SQL in staging queries, `select` sections, or `derivation` sections. 

Additionally, Chronon generates intermediate tables during data processing—for example, `Bootstrap` tables and `Join part` tables are created before producing the final `Join` tables.
The lineage parser identifies all output tables and features generated during data processing and provides column-level lineage.

Lineage data can serve various purposes, such as:
1. Automating personal data classification through tag propagation—automatically applying personal data tags from input columns to related output columns.
2. Displaying column-level lineage in the Lineage Graph View.
3. Tracing the root cause of data quality issues affecting specific output columns.
4. Enabling targeted backfills to address data quality issues in output columns.
5. Assessing the impact of deprecating an input column.

### Usage

You can pass in the config root folder path.
It parses the `teams.json` file and configs under the `production` folder to generate lineage metadata.

```python
from ai.chronon.lineage.lineage_parser import LineageParser

parser = LineageParser()
metadata = parser.parse_lineage("path_to_config_root_folder")
```

Metadata contains the following information:
- `metadata.lineages`: lineage from input_table/input_column to output_table/output_column.
- `metadata.features`: feature metadata including feature name, config name, and output column.
- `metadata.configs`: groupby/join/staging query configs, as well as output tables and features generated from each config.
- `metadata.tables`: input/output tables, including intermediate ones like join-part tables, bootstrap tables, staging query tables, etc.

More details can be found in `lineage_metadata.py`.

You can also access unparsed configs (due to format issues) from:
- `metadata.unparsed_configs`: configs that couldn't be parsed.
- `unparsed_columns`: columns without parsed lineage.

#### Parse With Schema Provider

You can optionally provide a schema provider when calling the lineage parser, as shown below:

```python
schema_provider: Optional[Callable[[str], Dict[str, str]]]
```

Knowing the schema is helpful when parsing queries with `SELECT *`, like:

```sql
SELECT a, b, c FROM (SELECT * FROM input_table) AS sub_table
```

Without a schema provider, such lineage can't be parsed.

#### Known Limitations

- Can't derive lineage from expressions like `COUNT(*) AS some_count` since the operation is on the table, not specific columns.
- The parser uses [SQLGlot](https://github.com/tobymao/sqlglot). In rare cases, SQLGlot may not parse correctly.
