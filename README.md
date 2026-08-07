# Walmart M5 Demand Forecasting on AWS

MGMT 599 · Group 5 · Jaewon Jang, Jason Tofte, Ross Roach

A serverless cloud analytics pipeline that turns the Walmart M5 retail dataset
into a 28-day demand forecast and an inventory-planning dashboard.

---

## The problem

A store inventory manager has to decide how much of each product to stock. Too
much ties up money in shelf space that could be spent elsewhere; too little means
lost sales. Both errors are expensive, and the manager makes this decision across
thousands of products at once.

This project forecasts daily unit demand so those decisions can be made against a
projection rather than a guess. The intended user is whoever sets store-level
stock levels.

## The data

[M5 Forecasting Accuracy](https://www.kaggle.com/competitions/m5-forecasting-accuracy)
(Kaggle), contributed by Walmart. Five CSV files covering **3,049 products across
10 stores in 3 states** (CA, TX, WI) over **1,941 days**.

| File | Contents |
|---|---|
| `sales_train_evaluation.csv` | 30,490 item-store series, one column per day (`d_1`…`d_1941`) |
| `calendar.csv` | 1,969 dates with holidays, events, and SNAP benefit days |
| `sell_prices.csv` | Weekly price per item, per store |
| `sales_train_validation.csv` | Same as evaluation, truncated at `d_1913` |
| `sample_submission.csv` | Competition format, unused here |

**Important:** the `d_` columns record **units sold** on that day, not on-hand
inventory. Roughly two thirds of item-days are zero — normal for slow-moving SKUs.

Raw data is **not committed to this repo** (400+ MB). Download it from Kaggle and
point the upload script at the folder.

## Architecture

![Pipeline](architecture/pipeline_diagram.png)

| Layer | AWS | GCP equivalent |
|---|---|---|
| Ingestion | CLI upload to S3 | Same (manual) |
| Storage | S3 — raw / processed / curated zones | Cloud Storage |
| Transformation | AWS Glue (Spark) + Glue Data Catalog | Dataflow |
| Query & analytics | Athena + Jupyter notebook | BigQuery |
| Output | QuickSight dashboard | Looker Studio |

Ingestion is a one-time manual upload rather than a streaming service, because
the dataset is fixed and never updates. Adding Kinesis here would be complexity
without benefit.

## Setup

**Prerequisites:** an AWS account, AWS CLI configured, Python 3.9+, and the M5
CSVs downloaded from Kaggle.

```bash
# 1. Create the bucket, zones, and upload the raw data
./scripts/upload_to_s3.sh /path/to/m5-forecasting-accuracy

# 2. Prove the transform locally on one store first (fast, ~30 seconds).
#    Do this before touching Glue — Glue cold-starts take 2-3 minutes per run.
python scripts/transform_local.py \
    --data-dir /path/to/m5-forecasting-accuracy \
    --out ./data/processed \
    --stores CA_1
```

**3. Run the transform at scale in Glue.** Create a Spark job from
`scripts/glue_job.py` with worker type G.1X, 3 workers, and job parameters
`--S3_BUCKET` and `--SALES_FILE`. The job validates the row count before writing
and fails loudly rather than producing bad output.

**4. Catalog the output.** Point a Glue crawler at
`s3://<bucket>/processed/sales_long/` targeting database `m5_db`. Confirm
`state_id` is detected as a **partition key**, not a regular column.

**5. Set the Athena query result location** to `s3://<bucket>/athena-results/`
under Athena → Settings. Athena will not run a single query until this is set.

**6. Run the SQL in order.** `sql/01_validation_rowcounts.sql` comes first —
if the row counts are wrong, everything downstream is wrong.

**7. Run the notebook.** `notebooks/02_forecasting_models.ipynb` pulls from
Athena, fits the models, and writes predictions back to S3.

**8. Build the dashboard.** In QuickSight, grant S3 and Athena access under
*Manage QuickSight → Security & permissions* — this is separate from your IAM
role and is the usual reason Athena tables do not appear.

## Repository map

```
architecture/     Pipeline diagram
scripts/          upload_to_s3.sh, transform_local.py, glue_job.py
sql/              Numbered Athena queries, run in order
notebooks/        EDA and forecasting models
screenshots/      Implementation evidence
docs/             Proposal, checkpoint, final report
```

### SQL files

| File | Purpose |
|---|---|
| `00_setup_database.sql` | Create `m5_db`, verify the crawler output |
| `01_validation_rowcounts.sql` | **Run first.** Row counts, PK uniqueness |
| `02_sales_by_category_store.sql` | Volume by category and store |
| `03_price_vs_demand.sql` | Does price move demand? |
| `04_snap_day_effect.sql` | SNAP benefit-day lift |
| `05_event_day_effect.sql` | Holiday and weekday seasonality |
| `06_data_quality_checks.sql` | Intermittency, nulls, outliers, lifecycle |
| `07_curated_daily_agg.sql` | Build the serving tables for QuickSight |
| `08_forecast_vs_actual.sql` | Register model output, score the models |

## Data model

One fact table at **item × store × day** grain — 59,181,090 rows, Snappy Parquet,
partitioned by `state_id`.

- **Primary key:** `(item_id, store_id, d)`
- **Calendar join:** `sales_long.d = calendar.d`
- **Price join:** `(store_id, item_id, wm_yr_wk)`, LEFT — nulls are real signal,
  meaning the item was not yet carried in that store

Partitioning is by state only, deliberately. Most queries filter to one state, so
a state filter skips roughly two thirds of the data. With under a gigabyte of
Parquet total, adding a year partition would create small-file overhead costing
more than it saves.

Curated tables (`daily_sales_agg`, `weekly_item_agg`, `forecast_vs_actual`) exist
so the dashboard never scans the 59M-row fact table.

## Results

*Populate from the notebook scorecard before submitting.*

| Model | RMSE | MAE | Bias |
|---|---|---|---|
| Seasonal naive (28d) | — | — | — |
| Naive (last value) | — | — | — |
| Weekday mean | — | — | — |
| Gradient boosting | — | — | — |

Key findings:

1. Demand is highly intermittent — roughly X% of item-days have zero sales.
2. Day of week is the dominant pattern, which is why a seasonal naive baseline is
   hard to beat.
3. SNAP benefit days lift FOODS demand by X%, with a much weaker effect on other
   categories — evidence the signal is real rather than an artifact.
4. Best model: X, at RMSE Y versus the baseline's Z.

## Interpreting the output

The dashboard shows historical daily units and a 28-day forecast, filterable by
state, store, and category. An inventory manager compares the forecast total for
the coming four weeks against current stock to decide whether to order.

Read **bias** alongside RMSE. A model that is accurate on average but
systematically under-forecasts is worse than its RMSE suggests, because a
stockout costs more than the equivalent excess inventory.

## Cost and cleanup

Total project cost: under $10.

- $10 AWS budget alert set before any resource was created
- Parquet + Snappy compression, cutting Athena scan volume by roughly 90%
- Partitioning so single-state queries skip two thirds of the data
- Curated aggregates so the dashboard never touches the fact table
- Notebook run locally rather than on a billed SageMaker instance

Athena is not the cost risk here — compression makes scanned volume trivial. The
real risks are idle resources: a forgotten notebook instance and an uncancelled
QuickSight subscription. Teardown removes the Glue job and crawler, empties the
S3 zones, and cancels QuickSight.

## Generative AI use

*Complete before submission — required by the assignment.* Describe what AI tools
were used for, what the group verified independently, and what was corrected. All
statistics in the report come from our own queries against our own data.
