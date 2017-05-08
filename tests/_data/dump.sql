CREATE TABLE IF NOT EXISTS "test_stat" (
    `event_date` Date,
    `time` Int32,
    `user_id` Int32
) ENGINE = MergeTree(event_date, (event_date, user_id), 8192);