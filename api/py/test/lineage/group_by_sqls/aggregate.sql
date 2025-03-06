SELECT
  subject,
  AGG(cnt) AS cnt_count,
  AGG(event_id) AS event_id_approx_percentile,
  AGG(event_id) AS event_id_last,
  AGG(event_id) AS event_id_sum
FROM agg_table