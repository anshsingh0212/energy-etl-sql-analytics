"""
kpi_report.py
Automated KPI report generator — reads from MDMS SQLite DB and
produces visual dashboard + CSV summaries for management reporting.

Mirrors the weekly Excel dashboard + KPI reporting done in production.
Author: Ansh Singh
"""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mtick
import seaborn as sns
import os

DB_PATH = "data/mdms.db"
os.makedirs("outputs", exist_ok=True)
sns.set_theme(style="whitegrid", palette="muted")

def run_query(query: str, conn) -> pd.DataFrame:
    return pd.read_sql(query, conn)
