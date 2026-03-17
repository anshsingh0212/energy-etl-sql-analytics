"""
etl_pipeline.py
Production-style ETL pipeline for smart meter data:
  Extract  → raw CSV sources (meter readings, consumer master, tariff table)
  Transform → clean, validate, enrich, aggregate
  Load     → SQLite MDMS-style database

Author: Ansh Singh
"""

import pandas as pd
import numpy as np
import sqlite3
import os, logging, json
from datetime import datetime, timedelta
from pathlib import Path

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("outputs/etl_pipeline.log"),
    ],
)
log = logging.getLogger(__name__)

DB_PATH = "data/mdms.db"
np.random.seed(42)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA GENERATORS (simulated raw source files)
# ══════════════════════════════════════════════════════════════════════════════

def generate_raw_data():
    log.info("Generating raw source data...")

    # Consumer master
    n = 200
    consumer_types = np.random.choice(
        ["residential", "commercial", "industrial", "agricultural"],
        size=n, p=[0.55, 0.25, 0.12, 0.08]
    )
    tariff_map = {
        "residential": "T1", "commercial": "T2",
        "industrial": "T3", "agricultural": "T4"
    }
    consumers = pd.DataFrame({
        "consumer_id"     : [f"CONS_{str(i).zfill(4)}" for i in range(1, n + 1)],
        "consumer_name"   : [f"Consumer_{i}" for i in range(1, n + 1)],
        "consumer_type"   : consumer_types,
        "tariff_code"     : [tariff_map[t] for t in consumer_types],
        "zone"            : np.random.choice(["ZONE_A", "ZONE_B", "ZONE_C"], size=n),
        "feeder_id"       : np.random.choice([f"F-0{i}" for i in range(1, 7)], size=n),
        "meter_serial_no" : [f"MSN{100000 + i}" for i in range(1, n + 1)],
        "installation_date": pd.date_range("2022-01-01", periods=n, freq="3D").strftime("%Y-%m-%d"),
        "contracted_demand_kva": np.where(
            consumer_types == "residential",  np.random.uniform(1, 5, n),
            np.where(consumer_types == "commercial",  np.random.uniform(5, 50, n),
                     np.where(consumer_types == "industrial", np.random.uniform(50, 500, n),
                              np.random.uniform(2, 15, n)))
        ).round(2),
        "status": np.random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "DISCONNECTED", "INACTIVE"], size=n),
    })

    # Tariff table
    tariffs = pd.DataFrame({
        "tariff_code"       : ["T1", "T2", "T3", "T4"],
        "tariff_name"       : ["Residential", "Commercial", "Industrial", "Agricultural"],
        "rate_per_unit"     : [6.50, 9.00, 7.50, 3.50],    # ₹ per kWh
        "fixed_charge_month": [100, 500, 2000, 80],          # ₹ per month
        "tod_peak_rate"     : [8.00, 11.00, 9.50, 4.50],    # ₹/kWh during peak
        "peak_hours_start"  : [18, 9, 8, 7],
        "peak_hours_end"    : [22, 20, 20, 12],
    })

    # Smart meter readings (daily summaries — like what MDMS stores after 15-min aggregation)
    start, end = datetime(2024, 10, 1), datetime(2025, 3, 31)
    dates = pd.date_range(start, end, freq="D")
    records = []
    for _, cons in consumers.iterrows():
        if cons["status"] == "INACTIVE":
            continue
        base = {"residential": 8, "commercial": 35, "industrial": 200, "agricultural": 25}[cons["consumer_type"]]
        for d in dates:
            wday = d.weekday()
            day_factor = 0.75 if wday >= 5 and cons["consumer_type"] in ("commercial", "industrial") else 1.0
            # Occasional missing data (comm failure)
            if np.random.rand() < 0.02:
                energy = np.nan
            else:
                energy = round(max(0, np.random.normal(base * day_factor, base * 0.15)), 3)

            records.append({
                "consumer_id"          : cons["consumer_id"],
                "reading_date"         : d.date().isoformat(),
                "energy_kwh"           : energy,
                "import_kwh"           : energy,
                "export_kwh"           : round(max(0, np.random.normal(0, 0.5)), 3),
                "max_demand_kva"       : round(energy / 24 * np.random.uniform(1.5, 2.5), 3) if energy else None,
                "power_factor_avg"     : round(np.random.uniform(0.85, 0.99), 3),
                "communication_status" : "OK" if energy else "FAILED",
                "reading_source"       : np.random.choice(["AMR", "AMI", "MANUAL"], p=[0.05, 0.90, 0.05]),
            })

    readings = pd.DataFrame(records)

    # Save raw files
    os.makedirs("data/raw", exist_ok=True)
    consumers.to_csv("data/raw/consumers.csv", index=False)
    tariffs.to_csv("data/raw/tariffs.csv", index=False)
    readings.to_csv("data/raw/meter_readings.csv", index=False)

    log.info(f"Raw data: {len(consumers)} consumers | {len(readings):,} readings | {len(tariffs)} tariffs")
    return consumers, tariffs, readings


# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACT
# ══════════════════════════════════════════════════════════════════════════════

def extract() -> dict:
    log.info("EXTRACT phase started")
    data = {
        "consumers": pd.read_csv("data/raw/consumers.csv"),
        "tariffs"  : pd.read_csv("data/raw/tariffs.csv"),
        "readings" : pd.read_csv("data/raw/meter_readings.csv"),
    }
    for name, df in data.items():
        log.info(f"  Extracted {name}: {len(df):,} rows, {len(df.columns)} cols")
    return data


# ══════════════════════════════════════════════════════════════════════════════
#  TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════

def validate_readings(df: pd.DataFrame) -> pd.DataFrame:
    """Data quality checks — mirrors MDMS billing validation logic."""
    dq = {"total": len(df), "missing_energy": 0, "negative_energy": 0,
          "comm_failures": 0, "duplicates": 0}

    dq["missing_energy"]  = df["energy_kwh"].isna().sum()
    dq["negative_energy"] = (df["energy_kwh"] < 0).sum()
    dq["comm_failures"]   = (df["communication_status"] == "FAILED").sum()
    dq["duplicates"]      = df.duplicated(["consumer_id", "reading_date"]).sum()

    dq["data_integrity_pct"] = round(
        (1 - dq["missing_energy"] / dq["total"]) * 100, 2
    )

    with open("outputs/data_quality_report.json", "w") as f:
        json.dump({k: int(v) if hasattr(v, 'item') else v for k, v in dq.items()}, f, indent=2)

    log.info(f"  Data Quality: {dq['data_integrity_pct']}% integrity | "
             f"{dq['missing_energy']:,} missing | {dq['comm_failures']:,} comm failures")
    return df


def transform_readings(readings: pd.DataFrame, consumers: pd.DataFrame,
                        tariffs: pd.DataFrame) -> pd.DataFrame:
    log.info("TRANSFORM phase started")

    df = readings.merge(consumers[["consumer_id", "consumer_type", "tariff_code",
                                    "zone", "feeder_id", "contracted_demand_kva"]],
                        on="consumer_id", how="left")
    df = df.merge(tariffs[["tariff_code", "rate_per_unit", "fixed_charge_month",
                             "tod_peak_rate"]],
                  on="tariff_code", how="left")

    df["reading_date"] = pd.to_datetime(df["reading_date"])

    # Fill missing with interpolation per consumer
    df = df.sort_values(["consumer_id", "reading_date"])
    df["energy_kwh"] = (
        df.groupby("consumer_id")["energy_kwh"]
          .transform(lambda x: x.interpolate(method="linear").fillna(method="ffill").fillna(method="bfill"))
    )

    # Billing columns
    df["billing_amount"]  = (df["energy_kwh"] * df["rate_per_unit"]).round(2)
    df["month_year"]      = df["reading_date"].dt.to_period("M").astype(str)
    df["day_of_week"]     = df["reading_date"].dt.day_name()
    df["is_weekend"]      = df["reading_date"].dt.dayofweek >= 5

    # Demand utilisation
    df["demand_utilisation_pct"] = (
        (df["max_demand_kva"] / df["contracted_demand_kva"]) * 100
    ).clip(0, 200).round(2)

    # Flag anomalies
    grp_mean = df.groupby("consumer_id")["energy_kwh"].transform("mean")
    grp_std  = df.groupby("consumer_id")["energy_kwh"].transform("std")
    df["is_anomaly"] = (
        (df["energy_kwh"] > grp_mean + 3 * grp_std) |
        (df["energy_kwh"] < 0) |
        (df["communication_status"] == "FAILED")
    ).astype(int)

    log.info(f"  Transformed {len(df):,} records | {df['is_anomaly'].sum():,} anomalies flagged")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  LOAD
# ══════════════════════════════════════════════════════════════════════════════

def load_to_db(consumers: pd.DataFrame, tariffs: pd.DataFrame,
               readings: pd.DataFrame, transformed: pd.DataFrame):
    log.info(f"LOAD phase started → {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    consumers.to_sql("dim_consumers", conn, if_exists="replace", index=False)
    tariffs.to_sql("dim_tariffs",     conn, if_exists="replace", index=False)
    readings.to_sql("fact_readings",  conn, if_exists="replace", index=False)
    transformed.to_sql("fact_billing", conn, if_exists="replace", index=False)

    # Monthly aggregated table
    monthly = (
        transformed.groupby(["consumer_id", "consumer_type", "zone", "month_year"])
        .agg(
            total_energy_kwh  = ("energy_kwh",     "sum"),
            avg_energy_kwh    = ("energy_kwh",     "mean"),
            billing_amount    = ("billing_amount", "sum"),
            comm_failure_days = ("is_anomaly",     "sum"),
            days_in_month     = ("energy_kwh",     "count"),
        )
        .reset_index()
    )
    monthly["data_availability_pct"] = (
        (monthly["days_in_month"] - monthly["comm_failure_days"]) /
         monthly["days_in_month"] * 100
    ).round(2)
    monthly.to_sql("fact_monthly_kpi", conn, if_exists="replace", index=False)

    # Create indexes for query performance
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_readings_consumer ON fact_readings(consumer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_readings_date ON fact_readings(reading_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_billing_month ON fact_billing(month_year)")
    conn.commit()

    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
    for t in tables["name"]:
        cnt = pd.read_sql(f"SELECT COUNT(*) as n FROM {t}", conn)["n"][0]
        log.info(f"  Loaded table '{t}': {cnt:,} rows")

    conn.close()
    log.info("LOAD phase complete")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    log.info("=" * 60)
    log.info("  Energy ETL Pipeline — Starting")
    log.info("=" * 60)
    t0 = datetime.now()

    consumers, tariffs, readings = generate_raw_data()
    raw = extract()
    validate_readings(raw["readings"])
    transformed = transform_readings(raw["readings"], raw["consumers"], raw["tariffs"])
    load_to_db(raw["consumers"], raw["tariffs"], raw["readings"], transformed)

    elapsed = (datetime.now() - t0).total_seconds()
    log.info(f"\n✓ Pipeline completed in {elapsed:.1f}s")
    log.info(f"  Database: {DB_PATH}")

    return transformed


if __name__ == "__main__":
    os.makedirs("outputs", exist_ok=True)
    run_pipeline()
