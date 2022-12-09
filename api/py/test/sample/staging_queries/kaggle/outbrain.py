from ai.chronon.api.ttypes import StagingQuery, MetaData

base_table = StagingQuery(
    query="""
        SELECT
            clicks_train.display_id,
            clicks_train.ad_id,
            clicks_train.clicked,
            events.document_id,
            CAST(events.timestamp as LONG) + 1465876799998 as ts,
            from_unixtime((CAST(events.timestamp as LONG) + 1465876799998)/1000, 'yyyy-MM-dd') as ds
        FROM
            kaggle_outbrain.clicks_train as clicks_train JOIN kaggle_outbrain.events as events ON clicks_train.display_id = events.display_id
    """,
    metaData=MetaData(
        name='outbrain_left',
        outputNamespace="default",
    )
)