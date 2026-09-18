# ETL Airflow — penjualan harian FMCG

**Nama:** Muhammad Aziiz Pranaja  
**NIM:** `25/572885/PPA/07200`  
**DAG:** `etl_sederhana_25_572885_PPA_07200`

Pipeline: `extract_data` → `validate_data` → `transform_data` → `load_data`.  
Pertanyaan bisnis: berapa total penjualan, kuantitas, dan jumlah order per hari?

## Prasyarat

- Docker Desktop (Windows)
- Apache Airflow **2.10.5** (image Docker, Python 3.12)
- pandas **2.1.4** (sudah ada di image Airflow; dikunci lewat constraints 2.10.5)

Airflow tidak didukung di Windows native. Jangan `pip install apache-airflow` di PowerShell.

## Struktur folder

```
airflow_tugas_25_572885_PPA_07200/
|-- dags/etl_sederhana_25_572885_PPA_07200.py
|-- data/raw/transactions.csv
|-- data/staging/          # extract & transform
|-- data/output/sales_daily.csv
|-- docker-compose.yml
|-- Dockerfile
`-- README.md
```

## Data raw

Letakkan CSV sumber di `data/raw/transactions.csv` (sudah disertakan, 1.800 baris).  
Kolom: `order_id`, `order_date`, `product`, `category`, `quantity`, `sales`, `region`.

## Menjalankan

```powershell
cd <folder_project>
docker compose up --build
```

- UI: http://localhost:8081 (jika 8080 terpakai; lihat mapping di `docker-compose.yml`)
- Login: `admin` / `admin`
- Unpause DAG `etl_sederhana_25_572885_PPA_07200`
- Trigger DAG (tombol Play)

Berhenti: `docker compose down`

File DAG sudah di-mount ke `/opt/airflow/dags`. Folder data di-mount ke `/opt/airflow/data`. Tidak perlu copy manual ke `AIRFLOW_HOME` jika memakai Docker Compose ini.

## Output

- `data/output/sales_daily.csv` — agregasi harian (wajib)
- `data/output/sales_by_category.csv` — agregasi per kategori (bonus)
- `docs/chart_total_sales_harian.png` — grafik penjualan harian
- `docs/chart_sales_by_category.png` — grafik penjualan per kategori

Run ulang **overwrite** file yang sama. Tutup Excel sebelum trigger ulang.
