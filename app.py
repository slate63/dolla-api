from fastapi import FastAPI, Query
from typing import Optional
import pandas as pd
from pathlib import Path

app = FastAPI(title="Dividend Scanner API")

DATA_DIR = Path("/data")
REQUIRED_COLUMNS = ['timestamp', 'symbol', 'dividends']

def has_required_columns_df(df, required_columns):
    return all(col in df.columns for col in required_columns)

@app.get("/scan-dividends")
async def scan_dividends(
    ticker: Optional[str] = Query(None, description="Optional ticker filter")
):
    results = []
    total_dividends = 0
    files_with_dividends = 0

    if not DATA_DIR.exists():
        return {"error": f"Data directory not found: {DATA_DIR}"}

    files = list(DATA_DIR.glob("*.parquet"))
    for file_path in files:
        try:
            df = pd.read_parquet(file_path)
            if not has_required_columns_df(df, REQUIRED_COLUMNS):
                continue

            df = df[REQUIRED_COLUMNS]
            if ticker:
                df = df[df['symbol'] == ticker.upper()]
            df = df[df['dividends'] != 0].copy()
            if not df.empty:
                df['file'] = file_path.name
                results.append(df)
                total_dividends += len(df)
                files_with_dividends += 1
        except Exception as e:
            continue

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df = final_df[['timestamp', 'symbol', 'dividends', 'file']]
        return {
            "files_scanned": len(files),
            "files_with_dividends": files_with_dividends,
            "total_dividends_found": total_dividends,
            "dividends": final_df.to_dict(orient="records")
        }
    else:
        return {
            "files_scanned": len(files),
            "files_with_dividends": 0,
            "total_dividends_found": 0,
            "dividends": []
        }
