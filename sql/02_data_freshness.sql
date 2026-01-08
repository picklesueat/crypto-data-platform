-- =============================================================================
-- Data Freshness: How current is our data across all coins?
-- =============================================================================

-- Data freshness per product (minutes since last trade)
SELECT 
    product_id,
    MAX(time) as latest_trade_time,
    date_diff('minute', MAX(time), current_timestamp) as minutes_since_last_trade,
    date_diff('hour', MAX(time), current_timestamp) as hours_since_last_trade,
    CASE 
        WHEN date_diff('minute', MAX(time), current_timestamp) < 60 THEN '🟢 Fresh (<1h)'
        WHEN date_diff('minute', MAX(time), current_timestamp) < 180 THEN '🟡 Stale (1-3h)'
        WHEN date_diff('minute', MAX(time), current_timestamp) < 1440 THEN '🟠 Old (3-24h)'
        ELSE '🔴 Very Old (>24h)'
    END as freshness_status,
    COUNT(*) as total_trades
FROM curated_trades
GROUP BY product_id
ORDER BY minutes_since_last_trade ASC;

-- =============================================================================
-- Average freshness across all coins
-- =============================================================================
SELECT 
    COUNT(DISTINCT product_id) as total_products,
    AVG(date_diff('minute', latest_time, current_timestamp)) as avg_minutes_stale,
    MIN(date_diff('minute', latest_time, current_timestamp)) as min_minutes_stale,
    MAX(date_diff('minute', latest_time, current_timestamp)) as max_minutes_stale,
    APPROX_PERCENTILE(date_diff('minute', latest_time, current_timestamp), 0.5) as median_minutes_stale,
    SUM(CASE WHEN date_diff('minute', latest_time, current_timestamp) < 60 THEN 1 ELSE 0 END) as fresh_products,
    SUM(CASE WHEN date_diff('minute', latest_time, current_timestamp) >= 60 
             AND date_diff('minute', latest_time, current_timestamp) < 180 THEN 1 ELSE 0 END) as stale_products,
    SUM(CASE WHEN date_diff('minute', latest_time, current_timestamp) >= 180 THEN 1 ELSE 0 END) as old_products
FROM (
    SELECT product_id, MAX(time) as latest_time
    FROM curated_trades
    GROUP BY product_id
);

-- =============================================================================
-- Products with stale data (>3 hours old) - needs attention
-- =============================================================================
SELECT 
    product_id,
    MAX(time) as latest_trade_time,
    date_diff('hour', MAX(time), current_timestamp) as hours_stale,
    COUNT(*) as total_trades,
    MAX(trade_id) as latest_trade_id
FROM curated_trades
GROUP BY product_id
HAVING date_diff('minute', MAX(time), current_timestamp) > 180
ORDER BY hours_stale DESC;
