import logging
from sqlalchemy import create_engine, text
from config import POSTGRES, LOG_FILE
from datetime import datetime

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_run(run_info):
    logging.info(run_info)
    engine = create_engine(f"postgresql+psycopg2://{POSTGRES['user']}:{POSTGRES['password']}@{POSTGRES['host']}:{POSTGRES['port']}/{POSTGRES['db']}")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_runs (
                id SERIAL PRIMARY KEY,
                date_run TIMESTAMP DEFAULT now(),
                lines_processed INT,
                lines_invalid INT,
                status VARCHAR(20),
                error TEXT
            )
        """))
        conn.execute(
            text("INSERT INTO etl_runs (lines_processed, lines_invalid, status, error) VALUES (:lines, :invalid, :status, :error)"),
            {
                "lines": run_info.get("lines_processed", 0),
                "invalid": run_info.get("lines_invalid", 0),
                "status": run_info.get("status", "SUCCESS"),
                "error": run_info.get("error", None)
            }
        )
