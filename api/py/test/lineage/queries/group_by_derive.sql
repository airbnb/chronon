SELECT
  * AS "*",
  event_id_sum + 1 AS event_id_sum_plus_one,
  event_id_last AS event_id_last_renamed,
  cnt_count,
  event_id_sum,
  event_id_approx_percentile,
  subject,
  ds
FROM (
  SELECT
    event_id_sum,
    event_id_last,
    cnt_count,
    event_id_approx_percentile,
    subject
  FROM transform_table
) AS transform_table