SELECT
  subject_sql AS subject,
  event_sql AS event_id,
  CAST(ts AS DOUBLE) AS ts
FROM join_event_table