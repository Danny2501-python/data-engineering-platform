from __future__ import annotations

import json
import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

TABLES = ["customers", "products", "orders", "order_items", "payments"]
SOURCE_ROOT = ROOT / "data" / "bronze" / "source"
TARGET_SCHEMA = "warehouse"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("e2e_warehouse_load")


def create_db_engine():
    required = ["NEON_HOST", "NEON_DATABASE", "NEON_USER", "NEON_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}. "
            "Check the local .env file; do not paste its password into chat."
        )

    url = URL.create(
        drivername="postgresql+psycopg",
        username=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        host=os.environ["NEON_HOST"],
        port=int(os.getenv("NEON_PORT", "5432")),
        database=os.environ["NEON_DATABASE"],
    )
    return create_engine(
        url,
        connect_args={
            "sslmode": os.getenv("NEON_SSLMODE", "require"),
            "connect_timeout": 15,
        },
        pool_pre_ping=True,
    )


def get_latest_run():
    summaries = sorted(SOURCE_ROOT.glob("run_*.json"), key=lambda path: path.name)
    if not summaries:
        raise FileNotFoundError(
            f"No run_*.json summary found under {SOURCE_ROOT}. "
            "Run python src/ingest.py first."
        )

    summary_path = summaries[-1]
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    entries = summary.get("tables", [])
    by_table = {entry.get("table"): entry for entry in entries}

    missing = [table for table in TABLES if table not in by_table]
    if missing:
        raise ValueError(
            f"Latest summary {summary_path.name} is missing tables: {missing}"
        )

    return summary_path, summary, by_table


def resolve_parquet_path(relative_path: str) -> Path:
    path = (ROOT / relative_path).resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise ValueError(f"Parquet path is outside the project folder: {relative_path}") from exc

    if not path.is_file():
        raise FileNotFoundError(f"Parquet file not found: {path}")
    return path


def main() -> int:
    engine = None
    try:
        summary_path, summary, entries = get_latest_run()
        run_id = str(summary.get("run_id", summary_path.stem.removeprefix("run_")))
        safe_run_id = re.sub(r"[^A-Za-z0-9_]", "_", run_id)[:20]

        logger.info("Using Bronze snapshot: %s", summary_path.name)
        engine = create_db_engine()

        with engine.connect() as conn:
            info = conn.execute(
                text("SELECT current_database(), current_user, now()")
            ).one()
            logger.info(
                "Connected | database=%s | user=%s | server_time=%s",
                info[0], info[1], info[2]
            )

        # Ensure the destination schema exists. This uses the same Neon database
        # as the source; the schemas provide logical separation only.
        with engine.begin() as conn:
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "warehouse"'))

        stage_names = {table: f"_stg_{table}_{safe_run_id}" for table in TABLES}
        expected_counts = {}

        # Load all five files into staging tables first. If this phase fails,
        # the current warehouse tables are left untouched.
        with engine.begin() as conn:
            for table in TABLES:
                entry = entries[table]
                parquet_path = resolve_parquet_path(entry["file"])
                expected_columns = entry.get("columns")
                if not expected_columns:
                    raise ValueError(
                        f"Manifest does not contain column names for {table}"
                    )

                # Parquet files live under Hive-style folders such as
                # load_date=YYYY-MM-DD/run_id=..., so PyArrow may infer these
                # partition keys as extra columns. Project only the actual
                # source columns recorded in the ingestion manifest.
                df = pd.read_parquet(
                    parquet_path,
                    engine="pyarrow",
                    columns=expected_columns,
                )
                if set(df.columns) != set(expected_columns):
                    missing_columns = sorted(set(expected_columns) - set(df.columns))
                    extra_columns = sorted(set(df.columns) - set(expected_columns))
                    raise ValueError(
                        f"Bronze column mismatch for {table}: "
                        f"missing={missing_columns}, extra={extra_columns}"
                    )
                # Keep manifest order deterministic for the Warehouse table.
                df = df[expected_columns]

                expected = int(entry["row_count"])
                if len(df) != expected:
                    raise ValueError(
                        f"Bronze row-count mismatch for {table}: "
                        f"manifest={expected}, parquet={len(df)}"
                    )

                stage = stage_names[table]
                logger.info(
                    "Loading %s rows from %s into %s.%s",
                    len(df), parquet_path.relative_to(ROOT), TARGET_SCHEMA, stage
                )
                df.to_sql(
                    name=stage,
                    con=conn,
                    schema=TARGET_SCHEMA,
                    if_exists="replace",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )

                actual = int(
                    conn.execute(
                        text(f' SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{stage}"')
                    ).scalar_one()
                )
                if actual != expected:
                    raise ValueError(
                        f"Staging row-count mismatch for {table}: "
                        f"expected={expected}, loaded={actual}"
                    )
                expected_counts[table] = expected

        # Refresh existing warehouse tables in place.
        # This preserves dependencies from dbt views.
        with engine.begin() as conn:
            existing_tables = set(
                conn.execute(
                    text(
                        """
                        SELECT tablename
                        FROM pg_catalog.pg_tables
                        WHERE schemaname = :schema
                        """
                    ),
                    {"schema": TARGET_SCHEMA},
                ).scalars().all()
            )

            # Delete child tables before parent tables.
            for table in reversed(TABLES):
                if table in existing_tables:
                    conn.execute(
                        text(
                            f'DELETE FROM "{TARGET_SCHEMA}"."{table}"'
                        )
                    )

            # Insert parent tables before child tables.
            for table in TABLES:
                stage = stage_names[table]

                if table in existing_tables:
                    conn.execute(
                        text(
                            f'INSERT INTO "{TARGET_SCHEMA}"."{table}" '
                            f'SELECT * FROM "{TARGET_SCHEMA}"."{stage}"'
                        )
                    )
                    conn.execute(
                        text(
                            f'DROP TABLE "{TARGET_SCHEMA}"."{stage}"'
                        )
                    )
                else:
                    # First load: no existing table to preserve.
                    conn.execute(
                        text(
                            f'ALTER TABLE "{TARGET_SCHEMA}"."{stage}" '
                            f'RENAME TO "{table}"'
                        )
                    )

                actual = int(
                    conn.execute(
                        text(
                            f'SELECT COUNT(*) FROM '
                            f'"{TARGET_SCHEMA}"."{table}"'
                        )
                    ).scalar_one()
                )

                expected = expected_counts[table]
                if actual != expected:
                    raise ValueError(
                        f"Warehouse row-count mismatch for {table}: "
                        f"expected={expected}, actual={actual}"
                    )

                conn.execute(
                    text(
                        f'ANALYZE "{TARGET_SCHEMA}"."{table}"'
                    )
                )

        logger.info("Warehouse load completed successfully.")
        for table in TABLES:
            logger.info("warehouse.%-12s %s rows", table, expected_counts[table])
        return 0

    except Exception:
        logger.exception("Warehouse load failed")
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
