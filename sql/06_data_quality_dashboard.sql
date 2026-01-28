-- =============================================================================
-- Data Quality Dashboard: Comprehensive health check
-- Run this query for a complete data quality overview
-- =============================================================================

-- =============================================================================
-- SECTION 1: Overall Summary
-- =============================================================================
SELECT 
    '📊 OVERALL SUMMARY' as section,
    COUNT(*) as total_records,
    COUNT(DISTINCT product_id) as products,
    ROUND(SUM(price * size), 2) as total_volume_usd,
    MIN(time) as earliest_data,
    MAX(time) as latest_data,
    date_diff('day', MIN(time), MAX(time)) as days_span
FROM curated_trades;

-- =============================================================================
-- SECTION 2: Data Freshness Summary
-- =============================================================================
WITH freshness AS (
    SELECT 
        product_id,
        MAX(time) as latest_time,
        date_diff('minute', MAX(time), current_timestamp) as minutes_stale
    FROM curated_trades
    GROUP BY product_id
)
SELECT 
    '⏰ FRESHNESS SUMMARY' as section,
    COUNT(*) as total_products,
    SUM(CASE WHEN minutes_stale < 60 THEN 1 ELSE 0 END) as fresh_lt_1h,
    SUM(CASE WHEN minutes_stale >= 60 AND minutes_stale < 180 THEN 1 ELSE 0 END) as stale_1h_3h,
    SUM(CASE WHEN minutes_stale >= 180 AND minutes_stale < 1440 THEN 1 ELSE 0 END) as old_3h_24h,
    SUM(CASE WHEN minutes_stale >= 1440 THEN 1 ELSE 0 END) as very_old_gt_24h,
    ROUND(AVG(minutes_stale), 2) as avg_minutes_stale,
    ROUND(APPROX_PERCENTILE(minutes_stale, 0.5), 2) as median_minutes_stale
FROM freshness;

-- =============================================================================
-- SECTION 3: Duplicate Summary
-- =============================================================================
SELECT 
    '🔁 DUPLICATE SUMMARY' as section,
    COUNT(*) as total_records,
    COUNT(DISTINCT CONCAT(product_id, '-', CAST(trade_id AS VARCHAR))) as unique_records,
    COUNT(*) - COUNT(DISTINCT CONCAT(product_id, '-', CAST(trade_id AS VARCHAR))) as duplicates,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT CONCAT(product_id, '-', CAST(trade_id AS VARCHAR)))) / COUNT(*), 4) as duplicate_pct
FROM curated_trades;

-- =============================================================================
-- SECTION 4: Trade ID Sequence Gap Detection
-- Uses trade_id sequence to detect missing trades (fast, exact)
-- =============================================================================
WITH trade_sequences AS (
    SELECT
        product_id,
        CAST(trade_id AS BIGINT) as trade_id,
        LEAD(CAST(trade_id AS BIGINT)) OVER (PARTITION BY product_id ORDER BY CAST(trade_id AS BIGINT)) - CAST(trade_id AS BIGINT) - 1 as missing_count
    FROM curated_trades
)
SELECT
    '📉 TRADE ID GAPS' as section,
    COUNT(DISTINCT product_id) as products_analyzed,
    SUM(CASE WHEN missing_count > 0 THEN missing_count ELSE 0 END) as total_missing_trades,
    COUNT(CASE WHEN missing_count > 0 THEN 1 END) as gap_occurrences,
    MAX(missing_count) as largest_single_gap,
    SUM(CASE WHEN missing_count > 10 THEN 1 ELSE 0 END) as gaps_over_10,
    SUM(CASE WHEN missing_count > 100 THEN 1 ELSE 0 END) as gaps_over_100
FROM trade_sequences;

-- =============================================================================
-- SECTION 5: Trade ID Completeness Summary
-- =============================================================================
WITH completeness AS (
    SELECT 
        product_id,
        MAX(trade_id) - MIN(trade_id) + 1 as expected,
        COUNT(DISTINCT trade_id) as actual,
        ROUND(100.0 * COUNT(DISTINCT trade_id) / (MAX(trade_id) - MIN(trade_id) + 1), 2) as pct
    FROM curated_trades
    GROUP BY product_id
)
SELECT 
    '✅ COMPLETENESS SUMMARY' as section,
    COUNT(*) as products,
    ROUND(AVG(pct), 2) as avg_completeness_pct,
    MIN(pct) as min_completeness_pct,
    SUM(CASE WHEN pct >= 99 THEN 1 ELSE 0 END) as products_99_plus_pct,
    SUM(CASE WHEN pct >= 95 AND pct < 99 THEN 1 ELSE 0 END) as products_95_99_pct,
    SUM(CASE WHEN pct < 95 THEN 1 ELSE 0 END) as products_below_95_pct
FROM completeness;

-- =============================================================================
-- SECTION 6: Top 10 Products by Volume
-- =============================================================================
SELECT 
    product_id,
    COUNT(*) as trade_count,
    ROUND(SUM(price * size), 2) as volume_usd,
    ROUND(AVG(price), 4) as avg_price,
    MIN(time) as first_trade,
    MAX(time) as last_trade,
    date_diff('minute', MAX(time), current_timestamp) as minutes_stale
FROM curated_trades
GROUP BY product_id
ORDER BY volume_usd DESC
LIMIT 10;

-- =============================================================================
-- SECTION 7: Problematic Products (stale or incomplete)
-- =============================================================================
WITH product_health AS (
    SELECT 
        product_id,
        COUNT(*) as trades,
        MAX(time) as latest,
        date_diff('minute', MAX(time), current_timestamp) as minutes_stale,
        ROUND(100.0 * COUNT(DISTINCT trade_id) / 
              NULLIF(MAX(trade_id) - MIN(trade_id) + 1, 0), 2) as completeness_pct
    FROM curated_trades
    GROUP BY product_id
)
SELECT 
    product_id,
    trades,
    latest as last_trade,
    minutes_stale,
    completeness_pct,
    CASE 
        WHEN minutes_stale > 1440 OR completeness_pct < 90 THEN '🔴 CRITICAL'
        WHEN minutes_stale > 180 OR completeness_pct < 95 THEN '🟠 WARNING'
        WHEN minutes_stale > 60 OR completeness_pct < 99 THEN '🟡 ATTENTION'
        ELSE '🟢 HEALTHY'
    END as health_status
FROM product_health
WHERE minutes_stale > 180 OR completeness_pct < 95
ORDER BY 
    CASE WHEN minutes_stale > 1440 OR completeness_pct < 90 THEN 1
         WHEN minutes_stale > 180 OR completeness_pct < 95 THEN 2
         ELSE 3 END,
    minutes_stale DESC;
