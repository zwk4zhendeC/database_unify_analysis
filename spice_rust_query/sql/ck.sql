CREATE DATABASE IF NOT EXISTS test_db;

CREATE TABLE IF NOT EXISTS test_db.city_perf
(
    id UInt64,
    city_name String,
    nation String
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE IF NOT EXISTS test_db.attacker_ip_perf
(
    id UInt64,
    attacker_ip String,
    city_id UInt64,
    city_name String
)
ENGINE = MergeTree()
ORDER BY id;
