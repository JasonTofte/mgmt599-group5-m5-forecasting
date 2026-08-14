-- ============================================================================
-- 02_sales_by_category_store.sql
-- Q1: Where does the volume actually come from?
-- ============================================================================
-- Purpose: the baseline picture for the dashboard, and the first real business
-- finding. Expect FOODS to dominate unit volume heavily; that shapes which
-- slice we scope the model to.
-- ============================================================================

SELECT
    state_id,
    store_id,
    cat_id,
    SUM(units_sold)                              AS total_units,
    ROUND(SUM(revenue), 0)                       AS total_revenue,
    ROUND(AVG(units_sold), 3)                    AS avg_units_per_item_day,
    COUNT(DISTINCT item_id)                      AS items_carried
FROM m5_db.sales_long
GROUP BY state_id, store_id, cat_id
ORDER BY state_id, store_id, total_units DESC;


-- ---------------------------------------------------------------------------
-- Category share of total units. The headline number for a slide
-- ---------------------------------------------------------------------------
SELECT
    cat_id,
    SUM(units_sold)                                          AS total_units,
    ROUND(100.0 * SUM(units_sold)
        / SUM(SUM(units_sold)) OVER (), 1)                   AS pct_of_all_units,
    ROUND(100.0 * SUM(revenue)
        / SUM(SUM(revenue)) OVER (), 1)                      AS pct_of_all_revenue
FROM m5_db.sales_long
GROUP BY cat_id
ORDER BY total_units DESC;
-- Note the gap between unit share and revenue share: FOODS moves the most
-- units but at lower prices. Worth a sentence in the findings section.


-- ---------------------------------------------------------------------------
-- Top 15 items by volume: candidates for a scoped modeling subset
-- ---------------------------------------------------------------------------
SELECT
    item_id,
    cat_id,
    SUM(units_sold)                    AS total_units,
    COUNT(DISTINCT store_id)           AS stores_carrying,
    ROUND(AVG(sell_price), 2)          AS avg_price
FROM m5_db.sales_long
GROUP BY item_id, cat_id
ORDER BY total_units DESC
LIMIT 15;
