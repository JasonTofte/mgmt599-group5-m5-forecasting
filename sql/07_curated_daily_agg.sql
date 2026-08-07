-- ============================================================================
-- 07_curated_daily_agg.sql
-- Build the curated tables that QuickSight reads.
-- ============================================================================
-- WHY THIS EXISTS: never point QuickSight at the 59M-row fact table. Every
-- filter change would re-scan it. This rollup is a few hundred thousand rows,
-- so the dashboard is instant and the Athena cost stays near zero.
--
-- This is also a legitimate architecture talking point: the curated zone is a
-- serving layer, deliberately denormalised and pre-aggregated for the BI tool.
-- ============================================================================

-- RE-RUNNING THIS FILE: DROP TABLE removes the catalog entry but NOT the S3
-- files. A second CTAS to the same external_location fails. Clear it first:
--   aws s3 rm s3://mgmt599-group5-m5/curated/daily_sales_agg/ --recursive
--   aws s3 rm s3://mgmt599-group5-m5/curated/weekly_item_agg/ --recursive

DROP TABLE IF EXISTS m5_db.daily_sales_agg;

CREATE TABLE m5_db.daily_sales_agg
WITH (
    format            = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://mgmt599-group5-m5/curated/daily_sales_agg/',
    partitioned_by    = ARRAY['state_id']    -- partition cols MUST be last in SELECT
) AS
SELECT
    date,
    year,
    month,
    wday,
    weekday,
    store_id,
    cat_id,
    dept_id,
    snap_flag,
    has_event,
    COALESCE(event_name_1, 'None')       AS event_name,
    SUM(units_sold)                      AS total_units,
    ROUND(SUM(revenue), 2)               AS total_revenue,
    ROUND(AVG(sell_price), 2)            AS avg_price,
    COUNT(DISTINCT item_id)              AS items_active,
    SUM(CASE WHEN units_sold > 0 THEN 1 ELSE 0 END) AS items_with_sales,
    state_id                             -- partition key last
FROM m5_db.sales_long
GROUP BY
    date, year, month, wday, weekday, store_id, cat_id, dept_id,
    snap_flag, has_event, COALESCE(event_name_1, 'None'), state_id;


-- Verify it built and is small enough for the dashboard
SELECT COUNT(*) AS agg_rows FROM m5_db.daily_sales_agg;
-- Expect a few hundred thousand, versus 59 million in the fact table.


-- ---------------------------------------------------------------------------
-- Item-level weekly rollup — for the model, not the dashboard.
-- Weekly grain reduces the zero-inflation problem substantially, which makes
-- the regression better behaved.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS m5_db.weekly_item_agg;

CREATE TABLE m5_db.weekly_item_agg
WITH (
    format            = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://mgmt599-group5-m5/curated/weekly_item_agg/'
) AS
SELECT
    item_id,
    store_id,
    state_id,
    cat_id,
    wm_yr_wk,
    MIN(date)                   AS week_start,
    SUM(units_sold)             AS weekly_units,
    MAX(sell_price)             AS sell_price,
    SUM(snap_flag)              AS snap_days_in_week,
    SUM(has_event)              AS event_days_in_week
FROM m5_db.sales_long
GROUP BY item_id, store_id, state_id, cat_id, wm_yr_wk;
