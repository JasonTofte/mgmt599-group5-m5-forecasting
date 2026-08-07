-- ============================================================================
-- 00_setup_database.sql
-- MGMT 599 Group 5 — one-time Athena setup
-- ============================================================================
-- Run these in order the first time you open Athena.
--
-- BEFORE ANY QUERY WILL RUN: set the query result location.
--   Athena console -> Settings -> Manage -> Location of query result:
--   s3://mgmt599-group5-m5/athena-results/
-- Athena silently refuses to run until this is set. This is the single most
-- common five-minute stumble.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS m5_db
COMMENT 'MGMT599 Group 5 - Walmart M5 demand forecasting';

-- ----------------------------------------------------------------------------
-- The fact table is created by the Glue crawler, not by hand.
--   Glue console -> Crawlers -> Create crawler
--   Data source : s3://mgmt599-group5-m5/processed/sales_long/
--   Target DB   : m5_db
--   Table name  : sales_long
-- The crawler should detect state_id as a PARTITION key, not a regular column.
-- If it lands as a regular column, the partitioning did not work — go back and
-- check the Glue job's partitionBy.
-- ----------------------------------------------------------------------------

-- After the crawler runs, confirm the schema:
SHOW CREATE TABLE m5_db.sales_long;

-- And confirm partitions are registered:
SHOW PARTITIONS m5_db.sales_long;
-- Expect exactly three: state_id=CA, state_id=TX, state_id=WI
