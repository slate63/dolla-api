from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import pandas as pd
from pathlib import Path
import logging
import time
import asyncio

app = FastAPI(title="Dividend & Stock Split Scanner API")

# CORS Middleware
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

SMA_PERIODS = [5, 20, 50, 100, 200]
EMA_PERIODS = [50, 100, 200]

def has_required_columns_df(df, required_columns):
    return all(col in df.columns for col in required_columns)

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("timestamp")
    for period in SMA_PERIODS:
        df[f"sma_{period}"] = df['close'].rolling(window=period).mean()
    for period in EMA_PERIODS:
        df[f"ema_{period}"] = df['close'].ewm(span=period, adjust=False).mean()
    return df

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
        value_column=None,
        endpoint_label='FULL_SCAN',
        full_data=True,
        filename_contains=filename_contains
    )

async def scan_generic(
    request: Request,
    ticker: Optional[str],
    required_columns,
    value_column: Optional[str],
    endpoint_label: str,
    full_data: bool,
    filename_contains: Optional[str]
):
    start_time = time.time()
    results = []
    total_dividends = 0
    total_stock_splits = 0
    files_with_data = 0
    files_with_errors = 0

    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        raise HTTPException(status_code=500, detail=f"Data directory not found: {DATA_DIR}")

    files = list(DATA_DIR.glob("*.parquet"))
    if filename_contains:
        files = [f for f in files if filename_contains.lower() in f.name.lower()]

    if not files:
        duration = round(time.time() - start_time, 2)
        return {
            "files_scanned": 0,
            "files_with_data": 0,
            "files_with_errors": 0,
            "elapsed_seconds": duration,
            "results": [],
            "message": "No files matched the given filters."
        }

    for file_path in files:
        try:
            df = await asyncio.to_thread(pd.read_parquet, file_path)
            if not has_required_columns_df(df, required_columns):
                continue

            df = df[ALL_COLUMNS if full_data else required_columns]
            if ticker:
                df = df[df['symbol'].str.upper() == ticker.upper()]
            if not full_data:
                if value_column in df.columns:
                    df = df[df[value_column] != 0].copy()
                    if value_column == 'dividends':
                        total_dividends += len(df)
                    elif value_column == 'stock splits':
                        total_stock_splits += len(df)
            if full_data and not df.empty:
                df = add_technical_indicators(df)

            if not df.empty:
                df['file'] = file_path.name
                results.append(df)
                files_with_data += 1

        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")
            files_with_errors += 1
            continue

    duration = round(time.time() - start_time, 2)

    # Log
    logger_line = (
        f"{endpoint_label} | ticker={ticker or 'ALL'} | files_scanned={len(files)} | "
        f"files_with_data={files_with_data} | files_with_errors={files_with_errors} | "
        f"duration_sec={duration} | ip={request.client.host}"
    )
    if not full_data and value_column == 'dividends':
        logger_line += f" | total_dividends={total_dividends}"
    elif not full_data and value_column == 'stock splits':
        logger_line += f" | total_stock_splits={total_stock_splits}"
    logger.info(logger_line)

    response = {
        "files_scanned": len(files),
        "files_with_data": files_with_data,
        "files_with_errors": files_with_errors,
        "elapsed_seconds": duration,
        "results": pd.concat(results, ignore_index=True).to_dict(orient="records") if results else []
    }

    if not full_data:
        if value_column == 'dividends':
            response["total_dividends"] = total_dividends
        elif value_column == 'stock splits':
            response["total_stock_splits"] = total_stock_splits

    return response
