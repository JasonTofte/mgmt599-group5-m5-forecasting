#!/usr/bin/env python3
"""
transform_local.py — M5 wide-to-long transform, run locally.

MGMT 599 Group 5. This is the prototype for the Glue job. Prove the logic here
on one store first, then port to Spark (see glue_job.py).

What it does:
  1. Reads sales_train_evaluation.csv (30,490 rows x 1,947 cols)
  2. Melts the d_1 ... d_1941 columns into (d, units_sold)
  3. Joins calendar.csv on d  -> date, wm_yr_wk, weekday features, events, SNAP
  4. Collapses snap_CA / snap_TX / snap_WI into a single snap_flag matching the
     row's own state (otherwise the model sees 3 columns where 1 is relevant)
  5. Joins sell_prices.csv on (store_id, item_id, wm_yr_wk) -> sell_price
  6. Validates row counts, then writes Parquet partitioned by state_id

Usage:
  # single store, fast, use this first
  python transform_local.py --data-dir ./data --out ./out --stores CA_1

  # one state
  python transform_local.py --data-dir ./data --out ./out --states CA

  # everything (~59.2M rows; needs ~8GB free RAM, run store-by-store instead
  # if it thrashes -- see --by-store)
  python transform_local.py --data-dir ./data --out ./out

  # safest full run: loops stores one at a time, low memory
  python transform_local.py --data-dir ./data --out ./out --by-store

Expected totals for the full evaluation file:
  30,490 series x 1,941 days = 59,181,090 rows
  CA 23,672,436 | TX 17,754,327 | WI 17,754,327
"""

import argparse
import os
import sys
import time

import numpy as np
import pandas as pd

N_DAYS_EVAL = 1941        # sales_train_evaluation.csv
N_DAYS_VALID = 1913       # sales_train_validation.csv
ID_COLS = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_calendar(data_dir):
    """Calendar is tiny (1,969 rows). Load fully, keep what we need."""
    cal = pd.read_csv(os.path.join(data_dir, "calendar.csv"))
    keep = [
        "d", "date", "wm_yr_wk", "weekday", "wday", "month", "year",
        "event_name_1", "event_type_1", "event_name_2", "event_type_2",
        "snap_CA", "snap_TX", "snap_WI",
    ]
    cal = cal[keep].copy()
    cal["date"] = pd.to_datetime(cal["date"])
    log(f"calendar loaded: {cal.shape[0]:,} rows")
    return cal


def load_prices(data_dir):
    """~6.8M rows. Downcast aggressively, this is the memory hog after sales."""
    prices = pd.read_csv(
        os.path.join(data_dir, "sell_prices.csv"),
        dtype={
            "store_id": "category",
            "item_id": "category",
            "wm_yr_wk": "int32",
            "sell_price": "float32",
        },
    )
    log(f"sell_prices loaded: {prices.shape[0]:,} rows")
    return prices


def load_sales(data_dir, filename, stores=None, states=None):
    """Load the wide sales file, optionally filtered to specific stores/states."""
    path = os.path.join(data_dir, filename)
    sales = pd.read_csv(path)

    n_day_cols = sum(1 for c in sales.columns if c.startswith("d_"))
    log(f"{filename} loaded: {sales.shape[0]:,} rows x {sales.shape[1]:,} cols "
        f"({n_day_cols:,} day columns)")

    if states:
        sales = sales[sales["state_id"].isin(states)]
        log(f"  filtered to states {states}: {sales.shape[0]:,} rows")
    if stores:
        sales = sales[sales["store_id"].isin(stores)]
        log(f"  filtered to stores {stores}: {sales.shape[0]:,} rows")

    if sales.empty:
        sys.exit("ERROR: filter matched zero rows. Check --stores / --states values.")

    return sales, n_day_cols


def melt_sales(sales):
    """Wide -> long. This is the core transform."""
    day_cols = [c for c in sales.columns if c.startswith("d_")]

    long_df = sales.melt(
        id_vars=[c for c in ID_COLS if c in sales.columns],
        value_vars=day_cols,
        var_name="d",
        value_name="units_sold",
    )

    # units_sold is small non-negative ints; int32 is plenty and halves memory
    long_df["units_sold"] = long_df["units_sold"].astype("int32")
    for c in ["item_id", "dept_id", "cat_id", "store_id", "state_id", "d"]:
        if c in long_df.columns:
            long_df[c] = long_df[c].astype("category")

    log(f"melted: {long_df.shape[0]:,} rows")
    return long_df


def resolve_snap(df):
    """
    Pick the SNAP column matching each row's own state.
    snap_CA/TX/WI -> single snap_flag. Vectorised via np.select so this stays
    fast on 59M rows (an .apply() here would take minutes).
    """
    state = df["state_id"].astype(str)
    conds = [state == "CA", state == "TX", state == "WI"]
    choices = [
        df["snap_CA"].to_numpy(),
        df["snap_TX"].to_numpy(),
        df["snap_WI"].to_numpy(),
    ]
    df["snap_flag"] = np.select(conds, choices, default=0).astype("int8")
    return df.drop(columns=["snap_CA", "snap_TX", "snap_WI"])


def transform(sales, cal, prices):
    """Melt + both joins + derived columns. Returns the analysis-ready frame."""
    n_series = len(sales)
    long_df = melt_sales(sales)
    rows_after_melt = len(long_df)

    # --- join calendar on d ---
    long_df = long_df.merge(cal, on="d", how="left")
    if len(long_df) != rows_after_melt:
        sys.exit(f"ERROR: calendar join changed row count "
                 f"{rows_after_melt:,} -> {len(long_df):,}. Duplicate d values?")
    log(f"calendar joined: {len(long_df):,} rows (unchanged, good)")

    long_df = resolve_snap(long_df)

    # --- join prices on (store_id, item_id, wm_yr_wk) ---
    # store_id/item_id are categoricals here and plain objects in prices;
    # cast both sides to str so the merge keys actually match.
    for c in ["store_id", "item_id"]:
        long_df[c] = long_df[c].astype(str)
    prices = prices.copy()
    for c in ["store_id", "item_id"]:
        prices[c] = prices[c].astype(str)

    long_df = long_df.merge(
        prices, on=["store_id", "item_id", "wm_yr_wk"], how="left"
    )
    if len(long_df) != rows_after_melt:
        sys.exit(f"ERROR: price join changed row count "
                 f"{rows_after_melt:,} -> {len(long_df):,}. "
                 f"sell_prices has duplicate (store,item,week) keys?")
    log(f"prices joined: {len(long_df):,} rows (unchanged, good)")

    # --- derived columns the model will want ---
    long_df["day_num"] = (
        long_df["d"].astype(str).str.replace("d_", "", regex=False).astype("int32")
    )
    long_df["has_event"] = long_df["event_name_1"].notna().astype("int8")
    long_df["revenue"] = (
        long_df["units_sold"] * long_df["sell_price"].fillna(0)
    ).astype("float32")

    long_df = long_df.sort_values(["store_id", "item_id", "day_num"])

    return long_df, n_series, rows_after_melt


def validate(df, n_series, n_days):
    """Checks worth screenshotting. Returns a small DataFrame of results."""
    checks = []

    expected = n_series * n_days
    checks.append((
        "row_count_matches_series_x_days",
        f"{expected:,}", f"{len(df):,}", len(df) == expected,
    ))

    null_units = int(df["units_sold"].isna().sum())
    checks.append(("units_sold_has_no_nulls", "0", f"{null_units:,}", null_units == 0))

    neg_units = int((df["units_sold"] < 0).sum())
    checks.append(("units_sold_non_negative", "0", f"{neg_units:,}", neg_units == 0))

    null_date = int(df["date"].isna().sum())
    checks.append(("calendar_join_complete", "0", f"{null_date:,}", null_date == 0))

    # Nulls here are EXPECTED (item not carried in that store yet), not a bug.
    null_price = int(df["sell_price"].isna().sum())
    pct = 100.0 * null_price / len(df)
    checks.append((
        "sell_price_nulls_expected_early_period",
        "informational", f"{null_price:,} ({pct:.1f}%)", True,
    ))

    zero_share = 100.0 * float((df["units_sold"] == 0).mean())
    checks.append((
        "zero_sales_intermittency",
        "informational", f"{zero_share:.1f}% of item-days", True,
    ))

    dupes = int(df.duplicated(subset=["item_id", "store_id", "d"]).sum())
    checks.append(("primary_key_unique", "0", f"{dupes:,}", dupes == 0))

    out = pd.DataFrame(checks, columns=["check_name", "expected", "actual", "passed"])

    log("")
    log("=" * 68)
    log("VALIDATION RESULTS  (screenshot this for the checkpoint)")
    log("=" * 68)
    print(out.to_string(index=False))
    log("=" * 68)

    by_state = (
        df.groupby("state_id", observed=True)
          .agg(rows=("units_sold", "size"),
               items=("item_id", "nunique"),
               stores=("store_id", "nunique"))
          .reset_index()
    )
    print("\nRow counts by state:")
    print(by_state.to_string(index=False))
    print()

    hard_fails = out[(~out["passed"]) & (out["expected"] != "informational")]
    if not hard_fails.empty:
        log("!!! HARD VALIDATION FAILURE — do not upload this output !!!")
        sys.exit(1)

    return out


def write_parquet(df, out_dir):
    """Partitioned by state_id, Snappy compressed — matches the Glue output."""
    os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(
        out_dir,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["state_id"],
        index=False,
    )
    total = sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, fs in os.walk(out_dir) for f in fs
    )
    log(f"wrote Parquet to {out_dir}  ({total / 1024**2:.1f} MB)")


def main():
    ap = argparse.ArgumentParser(description="M5 wide-to-long transform (local prototype)")
    ap.add_argument("--data-dir", default=".", help="folder holding the 5 M5 CSVs")
    ap.add_argument("--out", default="./processed", help="output folder for Parquet")
    ap.add_argument("--sales-file", default="sales_train_evaluation.csv",
                    choices=["sales_train_evaluation.csv", "sales_train_validation.csv"])
    ap.add_argument("--stores", nargs="*", default=None, help="e.g. CA_1 CA_2")
    ap.add_argument("--states", nargs="*", default=None, help="e.g. CA TX")
    ap.add_argument("--by-store", action="store_true",
                    help="loop one store at a time (low memory; use for full runs)")
    ap.add_argument("--skip-write", action="store_true", help="validate only")
    args = ap.parse_args()

    t0 = time.time()
    cal = load_calendar(args.data_dir)
    prices = load_prices(args.data_dir)

    if args.by_store:
        sales_all, n_days = load_sales(args.data_dir, args.sales_file,
                                       args.stores, args.states)
        all_checks = []
        for store in sorted(sales_all["store_id"].unique()):
            log("")
            log(f"--- {store} ---")
            chunk = sales_all[sales_all["store_id"] == store]
            df, n_series, _ = transform(chunk, cal, prices)
            all_checks.append(validate(df, n_series, n_days))
            if not args.skip_write:
                write_parquet(df, args.out)
            del df
        pd.concat(all_checks).to_csv(
            os.path.join(args.out, "_validation_summary.csv"), index=False
        )
    else:
        sales, n_days = load_sales(args.data_dir, args.sales_file,
                                   args.stores, args.states)
        df, n_series, _ = transform(sales, cal, prices)
        checks = validate(df, n_series, n_days)
        if not args.skip_write:
            write_parquet(df, args.out)
            checks.to_csv(os.path.join(args.out, "_validation_summary.csv"), index=False)

        print("\nSample of the output:")
        print(df[["item_id", "store_id", "date", "units_sold", "sell_price",
                  "snap_flag", "has_event"]].head(10).to_string(index=False))

    log(f"\ndone in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
