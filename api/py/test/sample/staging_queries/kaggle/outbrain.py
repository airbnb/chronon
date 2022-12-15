from ai.chronon.api.ttypes import StagingQuery, MetaData

base_table = StagingQuery(
    query="""
        SELECT
            clicks_train.display_id,
            clicks_train.ad_id,
            clicks_train.clicked,
            events.document_id,
            events.uuid,
            events.platform,
            events.geo_location,
            CAST(events.timestamp as LONG) + 1465876799998 as ts,
            FROM_UNIXTIME((CAST(events.timestamp as LONG) + 1465876799998)/1000, 'yyyy-MM-dd') as ds
        FROM
            kaggle_outbrain.clicks_train as clicks_train
        JOIN
            kaggle_outbrain.events as events
        ON clicks_train.display_id = events.display_id
    """,
    metaData=MetaData(
        name='outbrain_left',
        outputNamespace="default",
    )
)
