from ai.chronon.join import Join, JoinPart
from group_bys.kaggle.outbrain import ad_doc_features, outbrain_left_events

training_set = Join(  # left equi join
    left=outbrain_left_events(
        "display_id", "ad_id", "document_id", "clicked"),
    right_parts=[JoinPart(
        group_by=ad_doc_features
    )]
)
