-- ============================================================================
-- 04_snap_day_effect.sql
-- Q3 — Do SNAP benefit days lift demand? (Strongest business-relevant finding
--      in this dataset, and a genuine forecasting feature.)
-- ============================================================================
-- Background worth putting in the report: SNAP (food assistance) benefits are
-- distributed on fixed days of the month, and the distribution schedule differs
-- by state. If SNAP days lift food sales, an inventory manager can anticipate
-- those spikes rather than react to them — which is exactly the kind of
-- decision our stakeholder makes.
-- ============================================================================

SELECT
    state_id,
    cat_id,
    snap_flag,
    ROUND(AVG(units_sold), 4)                   AS avg_daily_units,
    COUNT(*)                                    AS item_days,
    SUM(units_sold)                             AS total_units
FROM m5_db.sales_long
GROUP BY state_id, cat_id, snap_flag
ORDER BY state_id, cat_id, snap_flag;


-- ---------------------------------------------------------------------------
-- The same thing as a single readable lift number per state and category
-- ---------------------------------------------------------------------------
WITH by_flag AS (
    SELECT
        state_id,
        cat_id,
        AVG(CASE WHEN snap_flag = 1 THEN units_sold END) AS avg_snap,
        AVG(CASE WHEN snap_flag = 0 THEN units_sold END) AS avg_non_snap
    FROM m5_db.sales_long
    GROUP BY state_id, cat_id
)
SELECT
    state_id,
    cat_id,
    ROUND(avg_snap, 4)                                          AS avg_units_snap_day,
    ROUND(avg_non_snap, 4)                                      AS avg_units_normal_day,
    ROUND(100.0 * (avg_snap - avg_non_snap) / avg_non_snap, 1)  AS pct_lift
FROM by_flag
ORDER BY pct_lift DESC;
-- Expect the lift to be clearly largest for FOODS, and smaller or near zero
-- for HOBBIES and HOUSEHOLD. That contrast is the finding: the effect is
-- specific to the category SNAP actually applies to, which is evidence the
-- signal is real and not an artifact.


-- ---------------------------------------------------------------------------
-- Does the SNAP effect vary by day of week? (Controls for the possibility that
-- SNAP days just happen to fall on high-traffic weekdays.)
-- ---------------------------------------------------------------------------
SELECT
    weekday,
    snap_flag,
    ROUND(AVG(units_sold), 4)   AS avg_daily_units,
    COUNT(*)                    AS item_days
FROM m5_db.sales_long
WHERE cat_id = 'FOODS'
GROUP BY weekday, snap_flag
ORDER BY weekday, snap_flag;
