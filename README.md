# End-to-End Data Engineering Pipeline

An end-to-end data engineering project that ingests transactional data from Neon PostgreSQL, stores Bronze-layer snapshots in Parquet, loads data into a warehouse, transforms it with dbt, and orchestrates the workflow using Apache Airflow.

## Project Overview

This project demonstrates a complete data pipeline, from source ingestion to analytics-ready data models and data quality testing.

**Key capabilities**

- Extract data from a Neon PostgreSQL source database.
- Store Bronze-layer snapshots as Parquet files with a manifest.
- Load ingested data into the data warehouse.
- Transform source data using dbt staging and mart models.
- Run dbt data quality tests.
- Orchestrate the workflow with Apache Airflow and Docker Compose.

## Architecture

```
flowchart LR
    A["Neon PostgreSQL<br/>Source schema"]
    B["Python ingestion<br/>src/ingest.py"]
    C["Bronze layer<br/>Parquet + manifest"]
    D["Warehouse loader<br/>src/load_warehouse.py"]
    E["Warehouse tables"]
    F["dbt staging models"]
    G["dbt mart models"]
    H["Analytics schema"]

    A --> B --> C --> D --> E --> F --> G --> H
    I["Apache Airflow"] -. orchestrates .-> B
    I -. orchestrates .-> D
    I -. orchestrates .-> F
    I -. orchestrates .-> G
```

## Technology Stack

| Technology      | Purpose                                    |
| --------------- | ------------------------------------------ |
| Python          | Data extraction and warehouse loading      |
| Neon PostgreSQL | Source database                            |
| Parquet         | Bronze-layer data storage                  |
| Apache Airflow  | Pipeline orchestration and task monitoring |
| Docker Compose  | Local containerized environment            |
| dbt             | SQL transformations and data quality tests |
| Git and GitHub  | Version control and project documentation  |

## Pipeline Workflow

The Airflow DAG is named `e2e_neon_to_warehouse`.

| Task                       | Description                                             |
| -------------------------- | ------------------------------------------------------- |
| `ingest_source_to_bronze`  | Extract source data and create Bronze Parquet snapshots |
| `load_bronze_to_warehouse` | Load Bronze data into warehouse tables                  |
| `dbt_run`                  | Execute dbt staging and mart transformations            |
| `dbt_test`                 | Run data quality tests against dbt models               |

The workflow runs these tasks in sequence. Each task must complete successfully before the next dependent task proceeds.

## Source Data

The sample source database contains the following tables:

| Table         | Sample row count |
| ------------- | ---------------- |
| `customers`   | 100              |
| `products`    | 50               |
| `orders`      | 1,000            |
| `order_items` | 3,000            |
| `payments`    | 1,000            |

These counts describe the current sample dataset and may change if the source data is updated.

## Data Modeling

### Staging models

The staging layer prepares source data for downstream transformations:

- `stg_customers`
- `stg_products`
- `stg_orders`
- `stg_order_items`
- `stg_payments`

### Analytics marts

The marts layer provides analytics-ready tables in the `analytics` schema.

| Model             | Purpose                     |
| ----------------- | --------------------------- |
| `dim_customers`   | Customer dimension          |
| `dim_products`    | Product dimension           |
| `fct_orders`      | Order-level fact table      |
| `fct_order_items` | Order-item-level fact table |

The dimension and fact models separate descriptive entities from transactional measures, making the data easier to use for reporting and analysis.

## Repository Structure

```
e2e-de-ingestion/
├── airflow/
│   ├── config/
│   │   └── dbt/
│   │       └── profiles.yml
│   ├── dags/
│   │   └── e2e_pipeline.py
│   ├── Dockerfile
│   └── docker-compose.yaml
├── models/
│   ├── staging/
│   └── marts/
├── sql/
│   └── verify_source.sql
├── src/
│   ├── ingest.py
│   └── load_warehouse.py
├── .env.example
├── .gitignore
├── dbt_project.yml
├── requirements.txt
└── README.md
```

Generated data, logs, dbt artifacts, virtual environments, and local secret files are excluded from version control.

## Getting Started

### Prerequisites

- Git
- Docker Desktop with Docker Compose
- A Neon PostgreSQL database with the required source tables

### 1. Clone the repository

```
git clone https://github.com/Danny2501-python/e2e-de-ingestion.git
cd e2e-de-ingestion
```

### 2. Configure environment variables

From the project root, create the root environment file:

```bash
cp .env.example .env
```

Edit `.env` and fill in your Neon PostgreSQL connection details:

```dotenv
NEON_HOST=
NEON_PORT=5432
NEON_DATABASE=
NEON_USER=
NEON_PASSWORD=
NEON_SSLMODE=require
```

Create the Airflow environment file:

```bash
cp airflow/.env.example airflow/.env
```

Generate a Fernet key using Python:

```bash
python -c "import secrets,base64; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Generate a separate JWT secret:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Copy the generated values into the matching variables in `airflow/.env`:
- `FERNET_KEY`
- `AIRFLOW__API_AUTH__JWT_SECRET`

Replace `_AIRFLOW_WWW_USER_PASSWORD` with a strong, unique password. Keep the remaining local path and image settings consistent with your environment.

**Never commit `.env` or `airflow/.env`.** The example files contain placeholders only; use unique secrets for each environment.

### 3. Start Airflow

From the project root:

```
cd airflow
docker compose up -d --build
```

Open the Airflow web interface:

[http://localhost:8080](http://localhost:8080)

Use the credentials configured for your local Airflow environment.

### 4. Run the pipeline

1. Open the Airflow web interface.
2. Find the `e2e_neon_to_warehouse` DAG.
3. Enable the DAG if it is paused.
4. Trigger a DAG run.
5. Monitor the task states and logs until the pipeline completes.

A successful run should show all four pipeline tasks as successful.

### 5. Stop the environment

From the `airflow` directory:

```
docker compose down
```

To inspect service logs:

```
docker compose logs -f
```

## Data Quality and Validation

The pipeline includes a dedicated `dbt_test` task after transformations.

The project also includes `sql/verify_source.sql` for source-data verification. Additional tests and business rules can be added as the project evolves.

## Security Considerations

- Store credentials in local environment files.
- Keep `.env` and `airflow/.env` out of version control.
- Do not commit database passwords, access tokens, or production data.
- Use `.env.example` to document required environment variable names without exposing their values.
- Use a dedicated database account with only the permissions required by the pipeline.

## Future Improvements

- Add incremental ingestion and change-data handling.
- Add automated CI checks for SQL, dbt models, and pipeline configuration.
- Improve retry policies, alerting, and operational monitoring.
- Add automated integration tests and pipeline performance metrics.

## Author

**Danny2501-python**

GitHub: [https://github.com/Danny2501-python](https://github.com/Danny2501-python)
