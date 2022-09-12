"""
Main extraction of statistics.

We rely on a provided sql -> pd.Dataframe implementation.
Ideally via presto or other "in memory" aggregation engine since the keys to aggregate are to be significantly smaller
than the input. However spark, hive or any other hook that takes sql and produces a pandas df should work.
"""


def extract_histogram(fetcher, column, featureTable, cap, ds, lookback):
    """
    Extract a capped histogram
    """
    first_ds = add_ds(ds, -1 * lookback)
    extract_query = f"""
    SELECT
      NUMERIC_HISTOGRAM({bins}, IF({column} <= {cap}, {column}, {cap})) as histogram_{column}
    FROM {featureTable.source_table}
    WHERE ds BETWEEN '{first_ds}' AND '{ds}'
    """
    return fetcher(sql)["histogram"][0]
