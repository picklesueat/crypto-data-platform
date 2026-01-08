-- =============================================================================
-- Data Overview: Total data size, record counts, and summary statistics
-- =============================================================================

-- Total records and data summary by product
SELECT 
    product_id,
    COUNT(*) as total_trades,
    COUNT(DISTINCT trade_id) as unique_trade_ids,
    MIN(time) as earliest_trade,
    MAX(time) as latest_trade,
    date_diff('day', MIN(time), MAX(time)) as days_of_data,
    SUM(price * size) as total_volume_usd,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM curated_trades
GROUP BY product_id
ORDER BY total_trades DESC;

-- =============================================================================
-- Overall totals
-- =============================================================================
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT product_id) as unique_products,
    COUNT(DISTINCT trade_id) as unique_trade_ids,
    MIN(time) as earliest_data,
    MAX(time) as latest_data,
    date_diff('day', MIN(time), MAX(time)) as total_days_span,
    SUM(price * size) as total_volume_usd
FROM curated_trades;
