"""DAG ETL sederhana untuk penjualan FMCG.

Mahasiswa : Muhammad Aziiz Pranaja
NIM       : 25/572885/PPA/07200
Tujuan    : extract -> validate -> transform -> load
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import os
import tempfile

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "transactions.csv"
STAGING_PATH = PROJECT_ROOT / "data" / "staging" / "extracted.csv"
TRANSFORMED_PATH = PROJECT_ROOT / "data" / "staging" / "transformed.csv"
TRANSFORMED_CATEGORY_PATH = PROJECT_ROOT / "data" / "staging" / "transformed_by_category.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "sales_daily.csv"
OUTPUT_CATEGORY_PATH = PROJECT_ROOT / "data" / "output" / "sales_by_category.csv"
CHART_PATH = PROJECT_ROOT / "docs" / "chart_total_sales_harian.png"
CHART_CATEGORY_PATH = PROJECT_ROOT / "docs" / "chart_sales_by_category.png"


def _atomic_to_csv(df, dest):
    """Tulis CSV overwrite lewat file sementara (aman di Windows/Docker)."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix=".csv",
        dir=dest.parent,
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, dest)
    except PermissionError as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise PermissionError(
            f"Gagal menulis {dest.name}. Tutup file itu di Excel/VS Code, lalu trigger ulang."
        ) from exc


def extract_data():
    """Baca CSV sumber dan salin ke staging tanpa mengubah file raw."""
    logging.info("[EXTRACT] mulai | file=%s", RAW_PATH)
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"CSV sumber tidak ditemukan: {RAW_PATH}")

    df = pd.read_csv(RAW_PATH)
    if df.empty:
        raise ValueError("File raw kosong; extract dihentikan.")

    _atomic_to_csv(df, STAGING_PATH)
    logging.info(
        "[EXTRACT] selesai | rows=%s | cols=%s | dest=%s",
        len(df),
        df.shape[1],
        STAGING_PATH,
    )
    logging.info("[EXTRACT] kolom=%s", df.columns.tolist())


def validate_data():
    """Validasi dasar sebelum transformasi: null dan duplicate.

    Temuan kualitas dicatat di log (warning), task tetap sukses.
    """
    logging.info("[VALIDATE] mulai | file=%s", STAGING_PATH)
    if not STAGING_PATH.exists():
        raise FileNotFoundError(f"File staging tidak ditemukan: {STAGING_PATH}")

    df = pd.read_csv(STAGING_PATH)
    required_cols = ["order_id", "order_date", "sales", "quantity"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Kolom wajib hilang di staging: {missing_cols}")

    null_count = df[required_cols].isnull().sum()
    null_total = int(null_count.sum())
    duplicate_count = int(df.duplicated().sum())
    duplicate_order_id = int(df["order_id"].duplicated().sum())

    logging.info("[VALIDATE] Null values:\n%s", null_count.to_string())
    logging.info(
        "[VALIDATE] null_total=%s | duplicates=%s | duplicate_order_id=%s",
        null_total,
        duplicate_count,
        duplicate_order_id,
    )

    if null_total or duplicate_count:
        logging.warning(
            "[VALIDATE] ada temuan kualitas | null_total=%s | duplicates=%s",
            null_total,
            duplicate_count,
        )
    else:
        logging.info("[VALIDATE] validasi lolos")

    logging.info("[VALIDATE] selesai | rows=%s", len(df))


def transform_data():
    """Agregasi penjualan per hari (wajib) dan per kategori (bonus)."""
    logging.info("[TRANSFORM] mulai | file=%s", STAGING_PATH)
    if not STAGING_PATH.exists():
        raise FileNotFoundError(f"File staging tidak ditemukan: {STAGING_PATH}")

    df = pd.read_csv(STAGING_PATH)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    nat_count = int(df["order_date"].isna().sum())
    logging.info("[TRANSFORM] tanggal_tidak_terparse=%s", nat_count)

    usable = df.dropna(subset=["order_date", "sales"]).copy()
    usable["quantity"] = pd.to_numeric(usable["quantity"], errors="coerce").fillna(0)
    usable["sales"] = pd.to_numeric(usable["sales"], errors="coerce")
    usable = usable[usable["quantity"] >= 0]
    usable["category"] = usable["category"].astype(str).str.strip().str.title()

    usable["order_date"] = usable["order_date"].dt.date
    daily = (
        usable.groupby("order_date", as_index=False)
        .agg(
            total_sales=("sales", "sum"),
            total_qty=("quantity", "sum"),
            n_orders=("order_id", "nunique"),
        )
        .sort_values("order_date")
    )

    by_category = (
        usable.groupby("category", as_index=False)
        .agg(
            total_sales=("sales", "sum"),
            total_qty=("quantity", "sum"),
            n_orders=("order_id", "nunique"),
        )
        .sort_values("total_sales", ascending=False)
    )

    _atomic_to_csv(daily, TRANSFORMED_PATH)
    _atomic_to_csv(by_category, TRANSFORMED_CATEGORY_PATH)

    logging.info(
        "[TRANSFORM] group_by=order_date | groups=%s | total_sales=%.2f | dest=%s",
        len(daily),
        float(daily["total_sales"].sum()),
        TRANSFORMED_PATH,
    )
    logging.info(
        "[TRANSFORM] group_by=category | groups=%s | dest=%s",
        len(by_category),
        TRANSFORMED_CATEGORY_PATH,
    )
    if len(daily):
        logging.info(
            "[TRANSFORM] rentang_tanggal=%s s.d. %s",
            daily["order_date"].min(),
            daily["order_date"].max(),
        )
    logging.info("[TRANSFORM] selesai")


def _write_sales_chart(daily_df):
    """Line chart total penjualan harian dari hasil akhir pipeline."""
    plot_df = daily_df.copy()
    plot_df["order_date"] = pd.to_datetime(plot_df["order_date"])
    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(plot_df["order_date"], plot_df["total_sales"], color="#1f4e79", linewidth=1.2)
    ax.set_title("Total penjualan harian FMCG")
    ax.set_xlabel("Tanggal")
    ax.set_ylabel("Total sales")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(CHART_PATH, dpi=120)
    plt.close(fig)
    logging.info("[LOAD] chart=%s", CHART_PATH)


def _write_category_chart(category_df):
    """Bar chart total penjualan per kategori dari hasil agregasi bonus."""
    plot_df = category_df.sort_values("total_sales", ascending=True)
    CHART_CATEGORY_PATH.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.barh(plot_df["category"], plot_df["total_sales"], color="#2e7d4f")
    ax.set_title("Total penjualan per kategori FMCG")
    ax.set_xlabel("Total sales")
    ax.set_ylabel("Kategori")
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(CHART_CATEGORY_PATH, dpi=120)
    plt.close(fig)
    logging.info("[LOAD] chart_category=%s", CHART_CATEGORY_PATH)


def load_data():
    """Simpan hasil agregasi ke folder output dan grafik (overwrite, idempotent)."""
    logging.info("[LOAD] mulai | file=%s", TRANSFORMED_PATH)
    if not TRANSFORMED_PATH.exists():
        raise FileNotFoundError(f"File hasil transform tidak ditemukan: {TRANSFORMED_PATH}")

    daily = pd.read_csv(TRANSFORMED_PATH)
    if daily.empty:
        raise ValueError("Hasil transform kosong; load dihentikan.")

    _atomic_to_csv(daily, OUTPUT_PATH)
    file_size = OUTPUT_PATH.stat().st_size
    if file_size <= 0:
        raise ValueError(f"File output kosong: {OUTPUT_PATH}")
    logging.info("[LOAD] written=%s | path=%s | size_bytes=%s", len(daily), OUTPUT_PATH, file_size)

    if TRANSFORMED_CATEGORY_PATH.exists():
        by_category = pd.read_csv(TRANSFORMED_CATEGORY_PATH)
        _atomic_to_csv(by_category, OUTPUT_CATEGORY_PATH)
        logging.info(
            "[LOAD] written_category=%s | path=%s",
            len(by_category),
            OUTPUT_CATEGORY_PATH,
        )
        _write_category_chart(by_category)

    _write_sales_chart(daily)
    logging.info("[LOAD] selesai")


default_args = {
    "owner": "25/572885/PPA/07200",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="etl_sederhana_25_572885_PPA_07200",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dwib", "etl", "fmcg"],
) as dag:
    extract = PythonOperator(task_id="extract_data", python_callable=extract_data)
    validate = PythonOperator(task_id="validate_data", python_callable=validate_data)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_data)
    load = PythonOperator(task_id="load_data", python_callable=load_data)

    extract >> validate >> transform >> load
