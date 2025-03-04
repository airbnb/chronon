SELECT
  test_group_by_event_id_approx_percentile,
  test_group_by_event_id_sum,
  test_group_by_cnt_count,
  test_group_by_event_id_sum_plus_one,
  event_id,
  test_group_by_event_id_last_renamed,
  subject,
  ts
FROM select_table
LEFT JOIN (
  SELECT
    test_group_by_event_id_approx_percentile AS test_group_by_event_id_approx_percentile,
    test_group_by_event_id_sum AS test_group_by_event_id_sum,
    test_group_by_event_id_sum_plus_one AS test_group_by_event_id_sum_plus_one,
    test_group_by_event_id_last_renamed AS test_group_by_event_id_last_renamed,
    test_group_by_cnt_count AS test_group_by_cnt_count,
    test_group_by_subject AS test_group_by_subject,
    subject AS subject
  FROM test_db.test_group_by
) AS test_group_by
  ON select_table.subject = test_group_by.subject
