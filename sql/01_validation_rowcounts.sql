-- ============================================================================
-- 01_validation_rowcounts.sql
-- RUN THIS FIRST, BEFORE ANY ANALYSIS. Screenshot the result.
-- ============================================================================
-- This is our headline data quality check and the single most valuable
-- screenshot for the checkpoint. If these numbers are wrong, every downstream
-- query and model is wrong, and we need to know now rather than on Sunday.
--
-- EXPECTED:
--   CA  23,672,436 rows | 3,049 items | 4 stores
--   TX  17,754,327 rows | 3,049 items | 3 stores
--   WI  17,754,327 rows | 3,049 items | 3 stores
--   TOTAL 59,181,090 rows = 30,490 series x 1,941 days
-- ============================================================================

SELECT
    state_id,
    COUNT(*)                    AS row_count,
    COUNT(DISTINCT item_id)     AS distinct_items,
    COUNT(DISTINCT store_id)    AS distinct_stores,
    COUNT(DISTINCT d)           AS distinct_days,
    MIN(date)                   AS first_date,
    MAX(date)                   AS last_date
FROM m5_db.sales_long
GROUP BY state_id
ORDER BY state_id;


-- ---------------------------------------------------------------------------
-- Grand total, and the arithmetic check spelled out
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                             AS actual_rows,
    COUNT(DISTINCT item_id) * COUNT(DISTINCT store_id)
        * COUNT(DISTINCT d)                              AS expected_rows,
    COUNT(*) = COUNT(DISTINCT item_id) * COUNT(DISTINCT store_id)
        * COUNT(DISTINCT d)                              AS passed
FROM m5_db.sales_long;


-- ---------------------------------------------------------------------------
-- Primary key uniqueness: (item_id, store_id, d) must be unique.
-- Returns zero rows if the key holds. Any rows returned = the join duplicated
-- records and the transform must be rerun.
-- ---------------------------------------------------------------------------
SELECT item_id, store_id, d, COUNT(*) AS n
FROM m5_db.sales_long
GROUP BY item_id, store_id, d
HAVING COUNT(*) > 1
LIMIT 20;
