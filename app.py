from fastapi import FastAPI, Query, Request
from typing import Optional
import pandas as pd
from pathlib import Path
import logging
import time

app = FastAPI(title="Dividend & Stock Split Scanner API")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("/data")
ALL_COLUMNS = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock splits']
DIVIDEND_COLS = ['timestamp', 'symbol', 'dividends']
SPLIT_COLS = ['timestamp', 'symbol', 'stock splits']

def has_required_columns_df(df, required_columns):
    return all(col in df.columns for col in required_columns)

@app.get("/scan-dividends")
async def scan_dividends(
    request: Request,
    ticker: Optional[str] = Query(None, description="Optional ticker filter")
):
    return await scan_generic(
        request=request,
        ticker=ticker,
        required_columns=DIVIDEND_COLS,
        value_column='dividends',
        endpoint_label='DIVIDEND_SCAN',
        full_data=False
    )

@app.get("/scan-splits")
async def scan_splits(
    request: Request,
    ticker: Optional[str] = Query(None, description="Optional ticker filter")
):
    return await scan_generic(
        request=request,
        ticker=ticker,
        required_columns=SPLIT_COLS,
        value_column='stock splits',
        endpoint_label='SPLIT_SCAN',
        full_data=False
    )

@app.get("/scan-splits-full")
async def scan_splits_full(
    request: Request,
    ticker: Optional[str] = Query(None, description="Optional ticker filter")
):
    return await scan_generic(
        request=request,
        ticker=ticker,
        required_columns=SPLIT_COLS,
        value_column='stock splits',
        endpoint_label='SPLIT_FULL_SCAN',
        full_data=True
    )

async def scan_generic(
    request: Request,
    ticker: Optional[str],
    required_columns,
    value_column: str,
    endpoint_label: str,
    full_data: bool
):
    start_time = time.time()
    results = []
    total_found = 0
    files_with_data = 0

    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        return {"error": f"Data directory not found: {DATA_DIR}"}

    files = list(DATA_DIR.glob("*.parquet"))

    for file_path in files:
        try:
            df = pd.read_parquet(file_path)
            if not has_required_columns_df(df, required_columns):
                continue

            df = df[ALL_COLUMNS if full_data else required_columns]
            if ticker:
                df = df[df['symbol'].str.upper() == ticker.upper()]
            df = df[df[value_column] != 0].copy()
            if not df.empty:
                df['file'] = file_path.name
                results.append(df)
                total_found += len(df)
                files_with_data += 1
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")
            continue

    duration = round(time.time() - start_time, 2)

    # One-line log
    logger.info(
        f"{endpoint_label} | ticker={ticker or 'ALL'} | files_scanned={len(files)} | "
        f"files_with_data={files_with_data} | total_{value_column.replace(' ', '_')}={total_found} | "
        f"duration_sec={duration} | ip={request.client.host}"
    )

    if results:
        final_df = pd.concat(results, ignore_index=True)
        return {
            "files_scanned": len(files),
            "files_with_data": files_with_data,
            f"total_{value_column.replace(' ', '_')}": total_found,
            "elapsed_seconds": duration,
            "results": final_df.to_dict(orient="records")
        }
    else:
        return {
            "files_scanned": len(files),
            "files_with_data": 0,
            f"total_{value_column.replace(' ', '_')}": 0,
            "elapsed_seconds": duration,
            "results": []
        }
