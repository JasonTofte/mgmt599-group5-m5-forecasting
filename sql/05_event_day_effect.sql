-- ============================================================================
-- 05_event_day_effect.sql
-- Q4 — Do holidays and events move demand? (Second candidate model feature.)
-- ============================================================================

-- Overall: event days vs normal days, by category
SELECT
    cat_id,
    has_event,
    ROUND(AVG(units_sold), 4)   AS avg_daily_units,
    COUNT(*)                    AS item_days
FROM m5_db.sales_long
GROUP BY cat_id, has_event
ORDER BY cat_id, has_event;


-- ---------------------------------------------------------------------------
-- By event TYPE — the aggregate above hides that different event types push
-- demand in opposite directions
-- ---------------------------------------------------------------------------
SELECT
    COALESCE(event_type_1, 'No event')  AS event_type,
    cat_id,
    ROUND(AVG(units_sold), 4)           AS avg_daily_units,
    COUNT(*)                            AS item_days
FROM m5_db.sales_long
GROUP BY COALESCE(event_type_1, 'No event'), cat_id
ORDER BY cat_id, avg_daily_units DESC;


-- ---------------------------------------------------------------------------
-- Named events ranked by their effect on FOODS demand.
-- Christmas is the one to watch: US Walmart stores close, so expect it to show
-- near-zero sales. That is a genuine data characteristic worth calling out in
-- the report, and it is a known trap for naive forecasting models.
-- ---------------------------------------------------------------------------
WITH baseline AS (
    SELECT AVG(units_sold) AS avg_normal
    FROM m5_db.sales_long
    WHERE cat_id = 'FOODS' AND event_name_1 IS NULL
)
SELECT
    s.event_name_1                                            AS event_name,
    MAX(s.event_type_1)                                       AS event_type,
    ROUND(AVG(s.units_sold), 4)                               AS avg_units_on_event,
    ROUND((SELECT avg_normal FROM baseline), 4)               AS avg_units_normal,
    ROUND(100.0 * (AVG(s.units_sold) - (SELECT avg_normal FROM baseline))
        / (SELECT avg_normal FROM baseline), 1)               AS pct_vs_normal,
    COUNT(DISTINCT s.date)                                    AS occurrences
FROM m5_db.sales_long s
WHERE s.cat_id = 'FOODS'
  AND s.event_name_1 IS NOT NULL
GROUP BY s.event_name_1
ORDER BY pct_vs_normal DESC;


-- ---------------------------------------------------------------------------
-- Day-of-week seasonality — almost certainly the strongest single pattern in
-- the data, and the reason a seasonal naive baseline is hard to beat
-- ---------------------------------------------------------------------------
SELECT
    wday,
    MAX(weekday)                AS weekday_name,
    ROUND(AVG(units_sold), 4)   AS avg_daily_units
FROM m5_db.sales_long
GROUP BY wday
ORDER BY wday;
