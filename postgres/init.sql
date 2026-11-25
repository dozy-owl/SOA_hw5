CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date   DATE             NOT NULL,
    metric_name   VARCHAR(50)      NOT NULL,
    dimension     VARCHAR(100)     NOT NULL DEFAULT '',
    metric_value  DOUBLE PRECISION NOT NULL,
    calculated_at TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, metric_name, dimension)
);