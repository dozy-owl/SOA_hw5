USE movie_analytics;

CREATE TABLE IF NOT EXISTS movie_events_queue (
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3),
    device_type String,
    session_id String,
    progress_seconds Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:29092,kafka-2:29094,kafka-3:29096',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-consumer-group',
    kafka_format = 'AvroConfluent',
    kafka_flush_interval_ms = 1000,
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS movie_events (
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3),
    device_type String,
    session_id String,
    progress_seconds Int32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, timestamp)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_mv TO movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds
FROM movie_events_queue;

CREATE TABLE IF NOT EXISTS agg_daily_metrics (
    metric_date Date,
    metric_name String,
    dimension String DEFAULT '',
    metric_value Float64,
    computed_at DateTime
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, metric_name, dimension);

CREATE TABLE IF NOT EXISTS agg_top_movies (
    metric_date Date,
    rank        UInt32,
    movie_id    String,
    views       UInt64,
    computed_at DateTime('UTC')
) ENGINE = MergeTree ORDER BY (metric_date, rank);