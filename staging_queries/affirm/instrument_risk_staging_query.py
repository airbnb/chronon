# affirm/staging_queries/instrument_risk_staging_query.py

from ai.chronon.api.ttypes import StagingQuery, MetaData

# StagingQuery automatically converts S3 YYYY/MM/DD partitioning to Chronon's expected 'ds' format
v1 = StagingQuery(
    query="""
        SELECT *, 
               CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) as ds
        FROM (
          SELECT *, 
                 INPUT_FILE_NAME() as file_path,
                 REGEXP_EXTRACT(INPUT_FILE_NAME(), '.*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/.*', 1) as year,
                 REGEXP_EXTRACT(INPUT_FILE_NAME(), '.*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/.*', 2) as month,
                 REGEXP_EXTRACT(INPUT_FILE_NAME(), '.*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/.*', 3) as day
          FROM parquet.`s3://affirm-risk-sherlock-ca/feature-store/instrument_risk_data/v1`
          WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
        )
    """,
    startPartition="2024-01-01",  # Adjust to your data start date
    metaData=MetaData(
        name="instrument_risk_staging_query",
        team="affirm",
        outputNamespace="affirm"
    )
)
