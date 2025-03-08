SELECT
  event_id_last + 1 AS event_id_last_plus_one_join,
  event_id,
  subject,
  test_group_by_cnt_count,
  test_group_by_event_id_approx_percentile,
  test_group_by_event_id_last_renamed,
  test_group_by_event_id_sum,
  test_group_by_event_id_sum_plus_one,
  ts
FROM (
  SELECT
    event_id,
    subject,
    test_group_by_cnt_count,
    test_group_by_event_id_approx_percentile,
    test_group_by_event_id_last_renamed,
    test_group_by_event_id_sum,
    test_group_by_event_id_sum_plus_one,
    ts
  FROM derive_table
) AS derive_table