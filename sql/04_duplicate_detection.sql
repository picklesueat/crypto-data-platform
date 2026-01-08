-- =============================================================================
-- Duplicate Detection: Find duplicate trades
-- =============================================================================

-- Check for duplicate trade_ids within each product
SELECT 
    product_id,
    trade_id,
    COUNT(*) as occurrence_count,
    MIN(time) as first_occurrence,
    MAX(time) as last_occurrence,
    COUNT(DISTINCT time) as unique_timestamps
FROM curated_trades
GROUP BY product_id, trade_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 100;

-- =============================================================================
-- Summary: Duplicate counts by product
-- =============================================================================
SELECT 
    product_id,
    COUNT(*) as total_records,
    COUNT(DISTINCT trade_id) as unique_trade_ids,
    COUNT(*) - COUNT(DISTINCT trade_id) as duplicate_count,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT trade_id)) / COUNT(*), 4) as duplicate_percentage
FROM curated_trades
GROUP BY product_id
HAVING COUNT(*) > COUNT(DISTINCT trade_id)
ORDER BY duplicate_count DESC;

-- =============================================================================
-- Overall duplicate summary
-- =============================================================================
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT CONCAT(product_id, '-', CAST(trade_id AS VARCHAR))) as unique_records,
    COUNT(*) - COUNT(DISTINCT CONCAT(product_id, '-', CAST(trade_id AS VARCHAR))) as total_duplicates,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT CONCAT(product_id, '-', CAST(trade_id AS VARCHAR)))) / COUNT(*), 4) as duplicate_pct
FROM curated_trades;
