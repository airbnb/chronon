SELECT
  test_join.event_id,
  test_join.subject,
  test_group_by_cnt_count,
  test_group_by_event_id_approx_percentile,
  test_group_by_event_id_last_renamed,
  test_group_by_event_id_sum,
  test_group_by_event_id_sum_plus_one,
  test_join.ts
FROM test_db.test_join_bootstrap AS test_join
LEFT JOIN (
  SELECT
    subject AS subject,
    cnt_count AS test_group_by_cnt_count,
    event_id_approx_percentile AS test_group_by_event_id_approx_percentile,
    event_id_last_renamed AS test_group_by_event_id_last_renamed,
    event_id_sum AS test_group_by_event_id_sum,
    event_id_sum_plus_one AS test_group_by_event_id_sum_plus_one
  FROM test_db.test_join_test_group_by
) AS gb_test_group_by
  ON test_join.subject = gb_test_group_by.subject