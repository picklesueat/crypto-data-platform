-- =============================================================================
-- Gap Detection: Find gaps in trade data compared to each coin's local average
-- 
-- A gap is defined as a period between trades that is significantly longer
-- than the coin's typical inter-trade interval (using standard deviations)
-- =============================================================================

-- Step 1: Calculate inter-trade intervals for each product
WITH trade_intervals AS (
    SELECT 
        product_id,
        trade_id,
        time as trade_time,
        LAG(time) OVER (PARTITION BY product_id ORDER BY time) as prev_trade_time,
        date_diff('second', LAG(time) OVER (PARTITION BY product_id ORDER BY time), time) as gap_seconds
    FROM curated_trades
),

-- Step 2: Calculate statistics per product (local average and stddev)
product_stats AS (
    SELECT 
        product_id,
        AVG(gap_seconds) as avg_gap_seconds,
        STDDEV(gap_seconds) as stddev_gap_seconds,
        APPROX_PERCENTILE(gap_seconds, 0.5) as median_gap_seconds,
        APPROX_PERCENTILE(gap_seconds, 0.95) as p95_gap_seconds,
        APPROX_PERCENTILE(gap_seconds, 0.99) as p99_gap_seconds,
        MAX(gap_seconds) as max_gap_seconds,
        COUNT(*) as total_intervals
    FROM trade_intervals
    WHERE gap_seconds IS NOT NULL 
      AND gap_seconds > 0
      AND gap_seconds < 86400 * 7  -- Ignore gaps > 7 days (likely data boundaries)
    GROUP BY product_id
),

-- Step 3: Identify anomalous gaps (> 3 standard deviations from mean)
anomalous_gaps AS (
    SELECT 
        ti.product_id,
        ti.trade_id,
        ti.trade_time,
        ti.prev_trade_time,
        ti.gap_seconds,
        ti.gap_seconds / 60.0 as gap_minutes,
        ti.gap_seconds / 3600.0 as gap_hours,
        ps.avg_gap_seconds,
        ps.stddev_gap_seconds,
        ps.median_gap_seconds,
        (ti.gap_seconds - ps.avg_gap_seconds) / NULLIF(ps.stddev_gap_seconds, 0) as z_score,
        CASE 
            WHEN (ti.gap_seconds - ps.avg_gap_seconds) / NULLIF(ps.stddev_gap_seconds, 0) > 5 THEN '🔴 EXTREME (>5σ)'
            WHEN (ti.gap_seconds - ps.avg_gap_seconds) / NULLIF(ps.stddev_gap_seconds, 0) > 4 THEN '🟠 SEVERE (>4σ)'
            WHEN (ti.gap_seconds - ps.avg_gap_seconds) / NULLIF(ps.stddev_gap_seconds, 0) > 3 THEN '🟡 WARNING (>3σ)'
            ELSE '🟢 NORMAL'
        END as gap_severity
    FROM trade_intervals ti
    JOIN product_stats ps ON ti.product_id = ps.product_id
    WHERE ti.gap_seconds IS NOT NULL
)

-- =============================================================================
-- Output: Anomalous gaps that stand out extremely (>3σ from local average)
-- =============================================================================
SELECT 
    product_id,
    prev_trade_time as gap_start,
    trade_time as gap_end,
    ROUND(gap_minutes, 2) as gap_minutes,
    ROUND(gap_hours, 2) as gap_hours,
    ROUND(avg_gap_seconds / 60.0, 2) as avg_gap_minutes,
    ROUND(z_score, 2) as z_score,
    gap_severity
FROM anomalous_gaps
WHERE z_score > 3  -- Only show gaps > 3 standard deviations
ORDER BY z_score DESC
LIMIT 100;

-- =============================================================================
-- Summary: Gap statistics by product
-- =============================================================================
SELECT 
    ps.product_id,
    ps.total_intervals,
    ROUND(ps.avg_gap_seconds / 60.0, 2) as avg_gap_minutes,
    ROUND(ps.median_gap_seconds / 60.0, 2) as median_gap_minutes,
    ROUND(ps.p95_gap_seconds / 60.0, 2) as p95_gap_minutes,
    ROUND(ps.p99_gap_seconds / 60.0, 2) as p99_gap_minutes,
    ROUND(ps.max_gap_seconds / 3600.0, 2) as max_gap_hours,
    (SELECT COUNT(*) FROM anomalous_gaps ag 
     WHERE ag.product_id = ps.product_id AND ag.z_score > 3) as anomalous_gap_count,
    (SELECT COUNT(*) FROM anomalous_gaps ag 
     WHERE ag.product_id = ps.product_id AND ag.z_score > 5) as extreme_gap_count
FROM product_stats ps
ORDER BY max_gap_seconds DESC;
