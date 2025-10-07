
from ai.chronon.api.ttypes import StagingQuery, MetaData

"""
StagingQuery to replicate the Feast materialization for Paybright repayment data.
This replaces the Airflow DAG: ml-ofs-galactus-materialize-paybright-repayment-data-dag

The DAG was materializing a Feast feature view called 'paybright_repayment_data_v1_0'
"""

# SQL query that replicates your Feast feature view materialization
query = """
SELECT 
    user_id,
    repayment_amount,
    transaction_id,
    merchant_id,
    repayment_date,
    country_code,
    status,
    ds  -- partition column for daily processing
FROM your_warehouse.paybright.repayment_data  -- Replace with your actual table
WHERE ds = '{{ ds }}'  -- This gets replaced with the actual date
  AND country_code = 'ca'  -- Based on your DAG's country filter
"""

v1 = StagingQuery(
    query=query,
    startPartition="2023-03-07",  # Matches your DAG's start_date
    metaData=MetaData(
        name='paybright_repayment_data_v1_0',  # Matches your FEATURE_VIEW_NAME
        outputNamespace="default",  # Adjust based on your namespace
        dependencies=[
            "your_warehouse.paybright.repayment_data/ds={{ ds }}"  # Replace with actual table
        ],
        tableProperties={
            "source": "chronon",
            "description": "Paybright repayment data materialization - replaces Airflow DAG"
        }
    )
)
