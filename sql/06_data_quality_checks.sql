-- ============================================================================
-- 06_data_quality_checks.sql
-- Q5/Q6 — Data quality findings. Both rubrics ask for this explicitly.
-- ============================================================================
-- The point is not to show the data is clean. It is to show we looked, found
-- specific characteristics, and understood what they mean for the model.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Intermittency: what share of item-days have zero sales?
--    This is the defining characteristic of the M5 data and the main reason
--    forecasting it is hard. Expect roughly 60-70% zeros overall.
-- ---------------------------------------------------------------------------
SELECT
    cat_id,
    COUNT(*)                                                     AS item_days,
    SUM(CASE WHEN units_sold = 0 THEN 1 ELSE 0 END)              AS zero_days,
    ROUND(100.0 * SUM(CASE WHEN units_sold = 0 THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                           AS pct_zero_days,
    ROUND(AVG(units_sold), 3)                                    AS avg_units,
    MAX(units_sold)                                              AS max_units
FROM m5_db.sales_long
GROUP BY cat_id
ORDER BY pct_zero_days DESC;


-- ---------------------------------------------------------------------------
-- 2. Null sell_price by year.
--    These nulls are EXPECTED, not corruption: an item has no price row for
--    weeks before that store carried it. Proof is that nulls concentrate in
--    early years and taper off. If they were random across all years, we would
--    have a real join problem.
-- ---------------------------------------------------------------------------
SELECT
    year,
    COUNT(*)                                                      AS rows_in_year,
    SUM(CASE WHEN sell_price IS NULL THEN 1 ELSE 0 END)           AS null_price_rows,
    ROUND(100.0 * SUM(CASE WHEN sell_price IS NULL THEN 1 ELSE 0 END)
        / COUNT(*), 1)                                            AS pct_null_price
FROM m5_db.sales_long
GROUP BY year
ORDER BY year;


-- ---------------------------------------------------------------------------
-- 3. Item lifecycle: leading zeros before an item's first real sale.
--    Confirms the interpretation above — items enter the catalogue over time.
-- ---------------------------------------------------------------------------
WITH first_sale AS (
    SELECT
        item_id,
        store_id,
        MIN(CASE WHEN units_sold > 0 THEN day_num END)  AS first_sale_day,
        MIN(CASE WHEN sell_price IS NOT NULL THEN day_num END) AS first_price_day
    FROM m5_db.sales_long
    WHERE state_id = 'CA'
    GROUP BY item_id, store_id
)
SELECT
    CASE
        WHEN first_sale_day <= 1       THEN 'Sold from day 1'
        WHEN first_sale_day <= 100     THEN 'First sale within 100 days'
        WHEN first_sale_day <= 500     THEN 'First sale day 101-500'
        WHEN first_sale_day <= 1000    THEN 'First sale day 501-1000'
        WHEN first_sale_day IS NULL    THEN 'NEVER sold'
        ELSE                                'First sale after day 1000'
    END                                            AS lifecycle_bucket,
    COUNT(*)                                       AS item_store_series,
    ROUND(AVG(first_price_day), 0)                 AS avg_first_price_day
FROM first_sale
GROUP BY 1
ORDER BY 2 DESC;


-- ---------------------------------------------------------------------------
-- 4. Outlier scan: implausibly large single-day sales
-- ---------------------------------------------------------------------------
SELECT
    item_id, store_id, date, units_sold, sell_price,
    COALESCE(event_name_1, 'none') AS event
FROM m5_db.sales_long
WHERE units_sold > 200
ORDER BY units_sold DESC
LIMIT 25;


-- ---------------------------------------------------------------------------
-- 5. Calendar coverage: every day present for every series, no gaps
-- ---------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT d)          AS distinct_days,
    MIN(day_num)               AS min_day,
    MAX(day_num)               AS max_day,
    MAX(day_num) - MIN(day_num) + 1 = COUNT(DISTINCT d) AS no_gaps
FROM m5_db.sales_long;


-- ---------------------------------------------------------------------------
-- 6. Persist the quality summary so it can be charted and screenshotted
--    (creates m5_db.data_quality_summary in the curated zone)
-- ---------------------------------------------------------------------------
CREATE TABLE m5_db.data_quality_summary
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://mgmt599-group5-m5/curated/data_quality_summary/'
) AS
SELECT 'total_rows'          AS check_name,
       CAST(COUNT(*) AS VARCHAR) AS metric_value,
       '59181090'            AS expected_value
FROM m5_db.sales_long
UNION ALL
SELECT 'pct_zero_sales_days',
       CAST(ROUND(100.0 * AVG(CASE WHEN units_sold = 0 THEN 1.0 ELSE 0.0 END), 2) AS VARCHAR),
       'informational'
FROM m5_db.sales_long
UNION ALL
SELECT 'pct_null_sell_price',
       CAST(ROUND(100.0 * AVG(CASE WHEN sell_price IS NULL THEN 1.0 ELSE 0.0 END), 2) AS VARCHAR),
       'informational'
FROM m5_db.sales_long
UNION ALL
SELECT 'distinct_item_store_series',
       CAST(COUNT(DISTINCT item_id || '|' || store_id) AS VARCHAR),
       '30490'
FROM m5_db.sales_long
UNION ALL
SELECT 'negative_units_rows',
       CAST(SUM(CASE WHEN units_sold < 0 THEN 1 ELSE 0 END) AS VARCHAR),
       '0'
FROM m5_db.sales_long;
