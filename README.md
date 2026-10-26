# End-to-End Data Engineering — Ingestion Starter

Initial flow: Neon PostgreSQL (`source` schema) -> Python -> local Parquet Bronze files.

This version performs full snapshots, not incremental CDC yet. Each run has a unique UTC run ID, so reruns do not overwrite prior snapshots.

## 1. Requirements
- Windows 10/11
- Python 3.10+ (3.11 or 3.12 recommended)
- Network access to Neon

Check Python in PowerShell:
```powershell
py --version
```

## 2. Configure Neon credentials
In PowerShell, from this project folder:
```powershell
Copy-Item .env.example .env
notepad .env
```
Fill in host, database, role, and password from Neon. Host must be only the hostname, without `postgresql://`, username, or `/neondb`. Keep `NEON_SSLMODE=require`. Never share `.env` or commit it to Git.

## 3. Create environment and install packages
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```
If activation is blocked, run this in the same PowerShell window and retry:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## 4. Run ingestion
```powershell
python src/ingest.py
```
The script tests the connection, extracts `customers`, `products`, `orders`, `order_items`, and `payments`, writes Parquet files plus per-table manifests, and logs row counts/errors.

Output example:
```text
data/bronze/source/customers/load_date=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSZ/customers.parquet
data/bronze/source/run_YYYYMMDDTHHMMSSZ.json
```

The `data/` directory is excluded by `.gitignore` because it contains extracted data.

## 5. Verify
Look for `.parquet` files under `data/bronze/source/`. Expected counts if the source started empty and the sample seed ran successfully: customers 100, products 50, orders 1000, order_items 3000, payments 1000. Existing rows may make counts higher.

## Next steps
1. Incremental extraction and persisted watermarks.
2. Separate warehouse target and idempotent loads.
3. dbt staging/intermediate/mart models and tests.
4. Airflow orchestration, retries, alerts, and SLA checks.
5. CI/CD.
6. Optionally replace local Bronze files with MinIO/S3 + Iceberg.

Do not commit passwords, `.env`, or real customer data.
