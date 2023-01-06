from ai.chronon.join import Join, JoinPart
from group_bys.kaggle.outbrain import ad_doc, ad_uuid, ad_platform, outbrain_left_events_sampled
from group_bys.kaggle.clicks import ad_streaming

training_set_2 = Join(  # left equi join
    left=outbrain_left_events_sampled(
        "uuid", "display_id", "ad_id", "document_id", "clicked", "geo_location", "platform"),
    right_parts=[JoinPart(group_by=group_by) for group_by in [ad_doc, ad_uuid, ad_streaming, ad_platform]]
)
