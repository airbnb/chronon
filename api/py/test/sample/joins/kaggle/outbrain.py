from ai.chronon.join import Join, JoinPart
from group_bys.kaggle.outbrain import ad_doc, ad_uuid, ad_platform
from sources.kaggle.outbrain import outbrain_left_events
from group_bys.kaggle.clicks import ad_streaming

training_set = Join(  # left equi join
    left=outbrain_left_events(
        "uuid", "display_id", "ad_id", "document_id", "clicked", "geo_location", "platform"),
    right_parts=[JoinPart(group_by=group_by) for group_by in [ad_doc, ad_uuid, ad_streaming, ad_platform]]
)
