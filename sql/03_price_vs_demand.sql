-- ============================================================================
-- 03_price_vs_demand.sql
-- Q2 — Does price actually move demand? (i.e. does sell_price earn a spot
--      in the regression model?)
-- ============================================================================
-- Method: within each item, bucket its observed prices into deciles, then
-- compare average daily units across buckets. Doing this WITHIN item matters —
-- comparing raw price to units across all items would just tell us cheap
-- categories sell more, which is not the same question.
-- ============================================================================

WITH priced AS (
    SELECT
        item_id,
        store_id,
        cat_id,
        sell_price,
        units_sold,
        NTILE(10) OVER (PARTITION BY item_id, store_id ORDER BY sell_price)
            AS price_decile
    FROM m5_db.sales_long
    WHERE sell_price IS NOT NULL
      AND state_id = 'CA'          -- partition filter keeps the scan small
)
SELECT
    price_decile,
    ROUND(AVG(sell_price), 2)      AS avg_price_in_decile,
    ROUND(AVG(units_sold), 3)      AS avg_daily_units,
    COUNT(*)                       AS observations
FROM priced
GROUP BY price_decile
ORDER BY price_decile;
-- Read it as: decile 1 = each item at its own cheapest, decile 10 = priciest.
-- A clean downward slope in avg_daily_units = price belongs in the model.


-- ---------------------------------------------------------------------------
-- Single-item deep dive — good for a slide, easier to explain than deciles
-- Swap the item_id for whatever came out top of query 02.
-- ---------------------------------------------------------------------------
SELECT
    sell_price,
    COUNT(*)                        AS days_at_this_price,
    ROUND(AVG(units_sold), 2)       AS avg_daily_units,
    MIN(date)                       AS first_seen,
    MAX(date)                       AS last_seen
FROM m5_db.sales_long
WHERE item_id  = 'FOODS_3_090'
  AND store_id = 'CA_3'
  AND sell_price IS NOT NULL
GROUP BY sell_price
ORDER BY sell_price;


-- ---------------------------------------------------------------------------
-- Price-change events: what happens to demand the week a price drops?
-- ---------------------------------------------------------------------------
WITH weekly AS (
    SELECT
        item_id, store_id, wm_yr_wk,
        MAX(sell_price)      AS price,
        SUM(units_sold)      AS weekly_units
    FROM m5_db.sales_long
    WHERE state_id = 'CA' AND sell_price IS NOT NULL
    GROUP BY item_id, store_id, wm_yr_wk
),
changes AS (
    SELECT
        *,
        LAG(price)       OVER (PARTITION BY item_id, store_id ORDER BY wm_yr_wk) AS prev_price,
        LAG(weekly_units) OVER (PARTITION BY item_id, store_id ORDER BY wm_yr_wk) AS prev_units
    FROM weekly
)
SELECT
    CASE
        WHEN price < prev_price * 0.9  THEN '1. price cut >10%'
        WHEN price < prev_price        THEN '2. price cut <10%'
        WHEN price = prev_price        THEN '3. no change'
        WHEN price > prev_price * 1.1  THEN '5. price rise >10%'
        ELSE                                '4. price rise <10%'
    END                                                AS price_move,
    COUNT(*)                                           AS week_count,
    ROUND(AVG(weekly_units), 2)                        AS avg_units_that_week,
    ROUND(AVG(prev_units), 2)                          AS avg_units_prior_week,
    ROUND(AVG(weekly_units) - AVG(prev_units), 2)      AS unit_lift
FROM changes
WHERE prev_price IS NOT NULL
GROUP BY 1
ORDER BY 1;
