SELECT
  event_id_sum + 1 AS event_id_sum_plus_one,
  event_id_last AS event_id_last_renamed,
  cnt_count,
  event_id_approx_percentile,
  event_id_sum,
  subject
FROM (
  SELECT
    cnt_count,
    event_id_approx_percentile,
    event_id_last,
    event_id_sum,
    subject
  FROM derive_table
) AS derive_table