SELECT
  AGG_APPROX_PERCENTILE(`event_id`) AS event_id_approx_percentile,
  AGG_COUNT(`cnt`) AS cnt_count,
  AGG_LAST(`event_id`) AS event_id_last,
  AGG_SUM(`event_id`) AS event_id_sum,
  subject
FROM agg_table