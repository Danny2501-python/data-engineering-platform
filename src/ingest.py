from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

TABLES = ["customers", "products", "orders", "order_items", "payments"]
SCHEMA = "source"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("e2e_ingestion")


def create_db_engine():
    required = ["NEON_HOST", "NEON_DATABASE", "NEON_USER", "NEON_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}. "
            "Copy .env.example to .env and fill in the connection details."
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


def main() -> int:
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    load_date = now.strftime("%Y-%m-%d")
    engine = None

    try:
        engine = create_db_engine()
        with engine.connect() as conn:
            info = conn.execute(
                text("SELECT current_database(), current_user, now()")
            ).one()
            logger.info(
                "Connected | database=%s | user=%s | server_time=%s",
                info[0], info[1], info[2]
            )

        run_summary = {
            "run_id": run_id,
            "extracted_at_utc": now.isoformat(),
            "source_database": os.getenv("NEON_DATABASE"),
            "source_schema": SCHEMA,
            "tables": [],
        }

        for table in TABLES:
            logger.info("Extracting %s.%s", SCHEMA, table)
            df = pd.read_sql_query(
                text(f'SELECT * FROM "{SCHEMA}"."{table}"'), engine
            )
            output_dir = (
                ROOT / "data" / "bronze" / "source" / table
                / f"load_date={load_date}" / f"run_id={run_id}"
            )
            output_dir.mkdir(parents=True, exist_ok=True)
            parquet_path = output_dir / f"{table}.parquet"
            df.to_parquet(parquet_path, index=False, engine="pyarrow")

            table_manifest = {
                "table": table,
                "source_schema": SCHEMA,
                "row_count": int(len(df)),
                "columns": list(df.columns),
                "extracted_at_utc": now.isoformat(),
                "file": str(parquet_path.relative_to(ROOT)).replace("\\", "/"),
                "format": "parquet",
            }
            (output_dir / "manifest.json").write_text(
                json.dumps(table_manifest, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            run_summary["tables"].append(table_manifest)
            logger.info("Saved %s rows to %s", len(df), parquet_path)

        summary_path = ROOT / "data" / "bronze" / "source" / f"run_{run_id}.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(run_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Ingestion completed. Summary: %s", summary_path)
        return 0

    except Exception:
        logger.exception("Ingestion failed")
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
