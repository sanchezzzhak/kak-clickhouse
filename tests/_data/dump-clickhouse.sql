CREATE TABLE IF NOT EXISTS `test_stat` (
    `event_date` Date,
    `time` Int32,
    `user_id` Int32,
    `active` Nullable(Int8),
    `test_uint64` UInt64,
    `test_int64` Int64,
    `test_ipv4` IPv4,
    `test_ipv6` IPv6,
    `test_uuid` UUID
) ENGINE = MergeTree(event_date, (event_date, user_id), 8192);
