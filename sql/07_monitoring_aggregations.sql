-- =============================================================================
-- Hourly/Daily Aggregations for Monitoring
-- =============================================================================

-- =============================================================================
-- Hourly trade counts - useful for detecting ingestion problems
-- =============================================================================
SELECT 
    product_id,
    date_trunc('hour', time) as hour,
    COUNT(*) as trade_count,
    SUM(price * size) as volume_usd,
    AVG(price) as avg_price,
    MIN(price) as low,
    MAX(price) as high
FROM curated_trades
WHERE time >= current_timestamp - interval '7' day
GROUP BY product_id, date_trunc('hour', time)
ORDER BY product_id, hour DESC;

-- =============================================================================
-- Hours with unusually low trade counts (potential ingestion gaps)
-- =============================================================================
WITH hourly_stats AS (
    SELECT 
        product_id,
        date_trunc('hour', time) as hour,
        COUNT(*) as trade_count
    FROM curated_trades
    WHERE time >= current_timestamp - interval '30' day
    GROUP BY product_id, date_trunc('hour', time)
),
product_hourly_avg AS (
    SELECT 
        product_id,
        AVG(trade_count) as avg_hourly_trades,
        STDDEV(trade_count) as stddev_hourly_trades
    FROM hourly_stats
    GROUP BY product_id
)
SELECT 
    hs.product_id,
    hs.hour,
    hs.trade_count,
    ROUND(pha.avg_hourly_trades, 2) as avg_hourly,
    ROUND((hs.trade_count - pha.avg_hourly_trades) / NULLIF(pha.stddev_hourly_trades, 0), 2) as z_score
FROM hourly_stats hs
JOIN product_hourly_avg pha ON hs.product_id = pha.product_id
WHERE (hs.trade_count - pha.avg_hourly_trades) / NULLIF(pha.stddev_hourly_trades, 0) < -2
ORDER BY z_score ASC
LIMIT 50;

-- =============================================================================
-- Daily summary for trending
-- =============================================================================
SELECT 
    DATE(time) as date,
    COUNT(DISTINCT product_id) as active_products,
    COUNT(*) as total_trades,
    SUM(price * size) as total_volume_usd,
    COUNT(*) / COUNT(DISTINCT product_id) as avg_trades_per_product
FROM curated_trades
WHERE time >= current_timestamp - interval '30' day
GROUP BY DATE(time)
ORDER BY date DESC;

-- =============================================================================
-- Ingestion latency: time between trade and ingestion
-- =============================================================================
SELECT 
    product_id,
    AVG(date_diff('second', time, ingested_at)) as avg_latency_seconds,
    MIN(date_diff('second', time, ingested_at)) as min_latency_seconds,
    MAX(date_diff('second', time, ingested_at)) as max_latency_seconds,
    APPROX_PERCENTILE(date_diff('second', time, ingested_at), 0.5) as median_latency_seconds,
    APPROX_PERCENTILE(date_diff('second', time, ingested_at), 0.95) as p95_latency_seconds
FROM curated_trades
WHERE time >= current_timestamp - interval '7' day
  AND ingested_at IS NOT NULL
GROUP BY product_id
ORDER BY avg_latency_seconds DESC
LIMIT 20;
