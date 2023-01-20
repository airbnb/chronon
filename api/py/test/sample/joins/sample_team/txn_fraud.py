from group_bys.sample_team import card_features
from group_bys.sample_team import merchant_features
from ai.chronon.api.ttypes import EventSource
from ai.chronon.join import Join, JoinPart

# Add data tests
v0 = Join(
    left=EventSource(
        table="namespace.your_driver_table"  # which contains merchant_ids and timestamps you want to backfill for
    ),
    right_parts=[
        JoinPart(group_by=card_features.v1),
        JoinPart(group_by=merchant_features.v1)],
    online=False,  # set to true if you want an endpoint
    # FUTURE(mid-feb): when we support derivations,
    # until then: You would need to post process in your service
    derivations={
        "z_score": "array_mean(map_values(map_zip_with(sample_team_card_features_v1_amount_average_by_card_id, sample_team_card_features_v1_amount_variance_by_card_id, (k, mean, variance) -> (transaction_value - mean)/variance)))",
        "txn_success_rate": "array_mean(map_values(merchant_features_v1_success_avg_by_ip))",
    }
)
