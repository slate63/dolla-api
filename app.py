from fastapi import FastAPI, Query, Request
from typing import Optional
import pandas as pd
from pathlib import Path
import logging
import time
from datetime import datetime

app = FastAPI(title="Dividend Scanner API")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("/data")
REQUIRED_COLUMNS = ['timestamp', 'symbol', 'dividends']

def has_required_columns_df(df, required_columns):
    return all(col in df.columns for col in required_columns)

@app.get("/scan-dividends")
async def scan_dividends(
    request: Request,
    ticker: Optional[str] = Query(None, description="Optional ticker filter")
):
    start_time = time.time()

    results = []
    total_dividends = 0
    files_with_dividends = 0

    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        return {"error": f"Data directory not found: {DATA_DIR}"}

    files = list(DATA_DIR.glob("*.parquet"))

    for file_path in files:
        try:
            df = pd.read_parquet(file_path)
            if not has_required_columns_df(df, REQUIRED_COLUMNS):
                continue

            df = df[REQUIRED_COLUMNS]
            if ticker:
                df = df[df['symbol'].str.upper() == ticker.upper()]
            df = df[df['dividends'] != 0].copy()
            if not df.empty:
                df['file'] = file_path.name
                results.append(df)
                total_dividends += len(df)
                files_with_dividends += 1
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")
            continue

    duration = round(time.time() - start_time, 2)

    # Log everything in a single structured line
    logger.info(
        f"DIVIDEND_SCAN | ticker={ticker or 'ALL'} | files_scanned={len(files)} | "
        f"files_with_dividends={files_with_dividends} | total_dividends={total_dividends} | "
        f"duration_sec={duration} | ip={request.client.host}"
    )

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df = final_df[['timestamp', 'symbol', 'dividends', 'file']]
        return {
            "files_scanned": len(files),
            "files_with_dividends": files_with_dividends,
            "total_dividends_found": total_dividends,
            "elapsed_seconds": duration,
            "dividends": final_df.to_dict(orient="records")
        }
    else:
        return {
            "files_scanned": len(files),
            "files_with_dividends": 0,
            "total_dividends_found": 0,
            "elapsed_seconds": duration,
            "dividends": []
        }
