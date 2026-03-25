create table test_db.city_perf
(
    id        UInt64,
    city_name String,
    nation    String
)
engine = MergeTree ORDER BY id
SETTINGS index_granularity = 8192;

