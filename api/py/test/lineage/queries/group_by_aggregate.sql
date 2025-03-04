SELECT
  AGG(ds) AS ds,
  AGG(event_id) AS event_id_sum,
  AGG(event_id) AS event_id_last,
  AGG(cnt) AS cnt_count,
  AGG(event_id) AS event_id_approx_percentile,
  AGG(subject) AS subject
FROM select_table