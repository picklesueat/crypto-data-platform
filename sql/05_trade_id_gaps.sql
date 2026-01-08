-- =============================================================================
-- Trade ID Sequence Gaps: Detect missing trade IDs within sequences
-- 
-- Identifies potential missed trades by finding gaps in trade_id sequences
-- =============================================================================

-- Find trade_id gaps (missing trade IDs in sequence)
WITH trade_sequences AS (
    SELECT 
        product_id,
        trade_id,
        time,
        LEAD(trade_id) OVER (PARTITION BY product_id ORDER BY trade_id) as next_trade_id,
        LEAD(trade_id) OVER (PARTITION BY product_id ORDER BY trade_id) - trade_id as id_gap
    FROM curated_trades
)

-- Show significant gaps in trade_id sequence (more than 1 missing)
SELECT 
    product_id,
    trade_id as last_seen_id,
    next_trade_id as next_seen_id,
    id_gap - 1 as missing_trade_count,
    time as last_seen_time
FROM trade_sequences
WHERE id_gap > 1
  AND next_trade_id IS NOT NULL
ORDER BY missing_trade_count DESC
LIMIT 100;

-- =============================================================================
-- Summary: Missing trades by product
-- =============================================================================
WITH trade_sequences AS (
    SELECT 
        product_id,
        trade_id,
        LEAD(trade_id) OVER (PARTITION BY product_id ORDER BY trade_id) - trade_id - 1 as missing_count
    FROM curated_trades
)
SELECT 
    product_id,
    COUNT(*) as total_trades,
    SUM(CASE WHEN missing_count > 0 THEN missing_count ELSE 0 END) as total_missing_trades,
    COUNT(CASE WHEN missing_count > 0 THEN 1 END) as gap_occurrences,
    MAX(missing_count) as largest_gap,
    ROUND(100.0 * SUM(CASE WHEN missing_count > 0 THEN missing_count ELSE 0 END) / 
          (MAX(trade_id) - MIN(trade_id)), 4) as estimated_missing_pct
FROM trade_sequences
GROUP BY product_id
HAVING SUM(CASE WHEN missing_count > 0 THEN missing_count ELSE 0 END) > 0
ORDER BY total_missing_trades DESC;

-- =============================================================================
-- Overall trade ID completeness
-- =============================================================================
SELECT 
    product_id,
    MIN(trade_id) as min_trade_id,
    MAX(trade_id) as max_trade_id,
    MAX(trade_id) - MIN(trade_id) + 1 as expected_trades,
    COUNT(DISTINCT trade_id) as actual_trades,
    MAX(trade_id) - MIN(trade_id) + 1 - COUNT(DISTINCT trade_id) as missing_trades,
    ROUND(100.0 * COUNT(DISTINCT trade_id) / (MAX(trade_id) - MIN(trade_id) + 1), 2) as completeness_pct
FROM curated_trades
GROUP BY product_id
ORDER BY completeness_pct ASC;
