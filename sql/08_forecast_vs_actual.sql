-- ============================================================================
-- 08_forecast_vs_actual.sql
-- Register the model output so QuickSight can chart forecast vs actual.
-- ============================================================================
-- CRITICAL STEP THAT IS EASY TO MISS: QuickSight can only chart what lives in
-- Athena. If the forecasts only exist as a pandas DataFrame in the notebook,
-- the forecast-vs-actual view we promised in the proposal cannot be built.
--
-- Workflow:
--   1. Notebook writes predictions to Parquet
--   2. Upload to s3://mgmt599-group5-m5/curated/forecast_vs_actual/
--   3. Run the CREATE EXTERNAL TABLE below (or point the crawler at it)
--   4. QuickSight -> new dataset -> Athena -> m5_db.forecast_vs_actual
--
-- Expected notebook output columns:
--   date, store_id, cat_id, item_id, actual_units, forecast_units, model_name
-- ============================================================================

DROP TABLE IF EXISTS m5_db.forecast_vs_actual;

-- NOTE: `date` is TIMESTAMP, not DATE. pandas writes datetime64 columns to
-- Parquet as timestamp[us]; declaring DATE here causes a type-mismatch error
-- on read. Verified against the transform output.
CREATE EXTERNAL TABLE m5_db.forecast_vs_actual (
    date            TIMESTAMP,
    store_id        STRING,
    cat_id          STRING,
    item_id         STRING,
    actual_units    DOUBLE,
    forecast_units  DOUBLE,
    model_name      STRING
)
STORED AS PARQUET
LOCATION 's3://mgmt599-group5-m5/curated/forecast_vs_actual/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- ---------------------------------------------------------------------------
-- Model scorecard — the numbers that go in the report and on a slide
-- ---------------------------------------------------------------------------
SELECT
    model_name,
    COUNT(*)                                                          AS predictions,
    ROUND(SQRT(AVG(POWER(actual_units - forecast_units, 2))), 4)      AS rmse,
    ROUND(AVG(ABS(actual_units - forecast_units)), 4)                 AS mae,
    ROUND(AVG(forecast_units - actual_units), 4)                      AS bias,
    ROUND(SUM(forecast_units) / NULLIF(SUM(actual_units), 0), 4)      AS total_ratio
FROM m5_db.forecast_vs_actual
GROUP BY model_name
ORDER BY rmse;
-- Report bias as well as RMSE. A model that is accurate on average but
-- systematically under-forecasts is worse for an inventory manager than the
-- RMSE alone suggests, because the cost of a stockout is not symmetric with
-- the cost of excess stock. That asymmetry is a good limitations paragraph.


-- ---------------------------------------------------------------------------
-- Daily comparison for the dashboard line chart
-- ---------------------------------------------------------------------------
SELECT
    date,
    store_id,
    cat_id,
    model_name,
    SUM(actual_units)     AS actual_units,
    SUM(forecast_units)   AS forecast_units,
    SUM(forecast_units) - SUM(actual_units) AS error
FROM m5_db.forecast_vs_actual
GROUP BY date, store_id, cat_id, model_name
ORDER BY date, store_id, cat_id;


-- ---------------------------------------------------------------------------
-- Where is the model worst? Useful for the limitations section.
-- ---------------------------------------------------------------------------
SELECT
    cat_id,
    model_name,
    ROUND(SQRT(AVG(POWER(actual_units - forecast_units, 2))), 4) AS rmse,
    ROUND(AVG(actual_units), 3)                                  AS avg_actual
FROM m5_db.forecast_vs_actual
GROUP BY cat_id, model_name
ORDER BY cat_id, rmse;
