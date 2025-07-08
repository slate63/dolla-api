from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import pandas as pd
from pathlib import Path
import logging
import time
import asyncio

app = FastAPI(title="Dividend & Stock Split Scanner API")

# CORS Middleware (optional for browser-based requests)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = Path("/data")
ALL_COLUMNS = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock splits']
DIVIDEND_COLS = ['timestamp', 'symbol', 'dividends']
SPLIT_COLS = ['timestamp', 'symbol', 'stock splits']

# Utility function
def has_required_columns_df(df, required_columns):
    return all(col in df.columns for col in required_columns)

# Root endpoint
@app.get("/")
def root():
    return {
        "message": "Welcome to the Dividend & Stock Split Scanner API",
        "endpoints": ["/scan-dividends", "/scan-splits", "/scan-full"]
    }

@app.get("/scan-dividends")
async def scan_dividends(
    request: Request,
    ticker: Optional[str] = Query(None),
    filename_contains: Optional[str] = Query(None)
):
    return await scan_generic(
        request=request,
        ticker=ticker,
        required_columns=DIVIDEND_COLS,
        value_column='dividends',
        endpoint_label='DIVIDEND_SCAN',
        full_data=False,
        filename_contains=filename_contains
    )

@app.get("/scan-splits")
async def scan_splits(
    request: Request,
    ticker: Optional[str] = Query(None),
    filename_contains: Optional[str] = Query(None)
):
    return await scan_generic(
        request=request,
        ticker=ticker,
        required_columns=SPLIT_COLS,
        value_column='stock splits',
        endpoint_label='SPLIT_SCAN',
        full_data=False,
        filename_contains=filename_contains
    )

@app.get("/scan-full")
async def scan_full(
    request: Request,
    ticker: Optional[str] = Query(None),
    filename_contains: Optional[str] = Query(None)
):
    return await scan_generic(
        request=request,
        ticker=ticker,
        required_columns=ALL_COLUMNS,
        value_column='dividends',  # still used for logging only
        endpoint_label='FULL_SCAN',
        full_data=True,
        filename_contains=filename_contains
    )

# Main logic
async def scan_generic(
    request: Request,
    ticker: Optional[str],
    required_columns,
    value_column: str,
    endpoint_label: str,
    full_data: bool,
    filename_contains: Optional[str]
):
    start_time = time.time()
    results = []
    total_found = 0
    files_with_data = 0

    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        raise HTTPException(status_code=500, detail=f"Data directory not found: {DATA_DIR}")

    files = list(DATA_DIR.glob("*.parquet"))
    if filename_contains:
        files = [f for f in files if filename_contains.lower() in f.name.lower()]

    for file_path in files:
        try:
            df = await asyncio.to_thread(pd.read_parquet, file_path)
            if not has_required_columns_df(df, required_columns):
                continue

            df = df[ALL_COLUMNS if full_data else required_columns]
            if ticker:
                df = df[df['symbol'].str.upper() == ticker.upper()]
            if not full_data and value_column in df.columns:
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
