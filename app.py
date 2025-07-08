from fastapi import FastAPI, Query, UploadFile, File
from typing import List, Optional
import pandas as pd
import pyarrow.parquet as pq
import os
from pathlib import Path
from io import BytesIO

app = FastAPI(title="Dividend Scanner API")

def has_required_columns_df(df, required_columns):
    return all(col in df.columns for col in required_columns)

@app.post("/scan-dividends")
async def scan_dividends(
    ticker: Optional[str] = Query(None, description="Optional ticker filter"),
    show_all: Optional[bool] = Query(False, description="Show all results"),
    files: List[UploadFile] = File(...)
):
    required_cols = ['timestamp', 'symbol', 'dividends']
    results = []
    total_dividends = 0
    files_with_dividends = 0

    for upload_file in files:
        try:
            # Read parquet file from upload
            contents = await upload_file.read()
            df = pd.read_parquet(BytesIO(contents), columns=None)
            
            if not has_required_columns_df(df, required_cols):
                continue
            
            df = df[required_cols]
            if ticker:
                df = df[df['symbol'] == ticker.upper()]
            df = df[df['dividends'] != 0]
            if not df.empty:
                df.loc[:, 'file'] = upload_file.filename
                results.append(df)
                total_dividends += len(df)
                files_with_dividends += 1
        except Exception as e:
            return {"error": f"Failed to process {upload_file.filename}: {str(e)}"}

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df = final_df[['timestamp', 'symbol', 'dividends', 'file']]
        if not show_all and len(final_df) > 20:
            final_df = final_df.head(20)
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
