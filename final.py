#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
smb_to_s3_sas_streamer.py

Enterprise-grade streamer to read a SAS7BDAT file directly from an SMB share and write
row-complete, schema-stable Parquet chunks to S3 / S3-compatible storage.

Key features
------------
- True streaming via sas7bdat iterator (no raw byte slicing)
- Memory-bounded adaptive chunking based on measured bytes/row
- Stable Arrow schema across all chunks
- UTF-8 normalization for string columns (Parquet requirement)
- SAS date/time/datetime normalization (SAS epoch 1960-01-01)
- Idempotent & resumable via manifest.json; emits _SUCCESS on completion
- S3-compatible endpoints (path-style addressing, v4 signing), multipart uploads
- Partition-by support (writes to prefix/col=value/part-*.parquet)
- Operational telemetry (rows/s, chunk index)

Limitations
-----------
- Compressed or encrypted SAS7BDAT (COMPRESS=YES) are not supported by sas7bdat.
  The script will detect likely compression and abort early with a clear message.
- Transport files (.xpt) and SPDE libraries are not supported by this path.
"""

import os
import sys
import gc
import json
import time
import math
import logging
from io import BytesIO
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

import smbclient
from sas7bdat import SAS7BDAT


# ----------------------------- Logging ---------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("sas-smb-s3")


# ----------------------------- Helpers ---------------------------------------

def safe_utf8(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize only object (string) columns to valid UTF-8."""
    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols) == 0:
        return df
    def norm(x):
        try:
            return x.encode("utf-8", errors="replace").decode("utf-8") if isinstance(x, str) else x
        except Exception:
            return x
    df[obj_cols] = df[obj_cols].applymap(norm)
    return df


def normalize_sas_dates(df: pd.DataFrame,
                        date_cols: Optional[List[str]] = None,
                        time_cols: Optional[List[str]] = None,
                        datetime_cols: Optional[List[str]] = None) -> pd.DataFrame:
    """Convert SAS numeric date/time/datetime to pandas types (SAS epoch 1960-01-01)."""
    SAS_EPOCH = pd.Timestamp("1960-01-01")
    date_cols = date_cols or []
    time_cols = time_cols or []
    datetime_cols = datetime_cols or []

    # If nothing specified, best-effort inference by suffix
    if not (date_cols or time_cols or datetime_cols):
        for c in df.columns:
            lc = str(c).lower()
            if lc.endswith(("_date", "date", "dt")):           # e.g., as_of_date
                date_cols.append(c)
            elif lc.endswith(("_time", "time", "tm")):
                time_cols.append(c)
            elif lc.endswith(("datetime", "timestamp", "ts")):
                datetime_cols.append(c)

    # Apply conversions where dtype is numeric
    for c in date_cols:
        if c in df.columns and pd.api.types.is_numeric_dtype(df[c]):
            df[c] = pd.to_datetime(SAS_EPOCH + pd.to_timedelta(df[c], unit="D"), errors="coerce")
    for c in time_cols:
        if c in df.columns and pd.api.types.is_numeric_dtype(df[c]):
            df[c] = pd.to_timedelta(df[c], unit="s")
    for c in datetime_cols:
        if c in df.columns and pd.api.types.is_numeric_dtype(df[c]):
            df[c] = pd.to_datetime(SAS_EPOCH + pd.to_timedelta(df[c], unit="s"), errors="coerce")
    return df


def bytes_per_row(sample_df: pd.DataFrame) -> float:
    """Compute average bytes per row for adaptive chunk sizing."""
    n = max(len(sample_df), 1)
    bpr = float(sample_df.memory_usage(deep=True).sum()) / n
    if not math.isfinite(bpr) or bpr <= 0:
        bpr = 512.0
    return bpr


def derive_chunk_rows(target_mb: int, bpr: float,
                      floor: int = 5_000, ceil: int = 250_000) -> int:
    rows = int((target_mb * 1024 * 1024) / bpr)
    return max(floor, min(ceil, rows))


def s3_client():
    """Create S3 or S3-compatible client."""
    cfg = Config(
        s3={"addressing_style": "path"},
        signature_version="s3v4",
        retries={"max_attempts": 10, "mode": "adaptive"}
    )
    kwargs = dict(
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=cfg
    )
    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


def transfer_cfg():
    """Multipart transfer configuration (tunable)."""
    chunk_mb = int(os.getenv("MULTIPART_CHUNK_MB", "16"))
    max_conc = int(os.getenv("MAX_CONCURRENCY", "8"))
    return TransferConfig(
        multipart_threshold=chunk_mb * 1024 * 1024,
        multipart_chunksize=chunk_mb * 1024 * 1024,
        max_concurrency=max_conc,
        use_threads=True,
    )


def load_manifest(s3, bucket: str, key: str):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {"chunks": [], "schema": None, "row_count": None}
    except ClientError:
        return {"chunks": [], "schema": None, "row_count": None}


def save_manifest(s3, bucket: str, key: str, manifest: dict):
    s3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(manifest, indent=2).encode("utf-8"))


# ----------------------------- Core ------------------------------------------

def stream_sas_from_smb_to_s3():
    """
    Main entrypoint: streams one SAS7BDAT from SMB to S3 as Parquet chunks.
    All configuration comes from environment variables (see README section below).
    """
    # --- Config via ENV ---
    SMB_SERVER   = os.getenv("SMB_SERVER")
    SMB_SHARE    = os.getenv("SMB_SHARE")              # share name only
    SMB_PATH     = os.getenv("SMB_FILE_PATH")          # path inside the share
    SMB_USER     = os.getenv("SMB_USERNAME")
    SMB_PASS     = os.getenv("SMB_PASSWORD")

    BUCKET       = os.getenv("S3_BUCKET")
    PREFIX       = os.getenv("S3_PREFIX", "sas_ingest/")
    MANIFEST     = os.getenv("MANIFEST_NAME", "_manifest.json")

    MAX_MB       = int(os.getenv("MAX_MEMORY_MB", "3000"))
    PROBE_ROWS   = int(os.getenv("PROBE_ROWS", "2000"))     # safer default for 1k+ columns
    FORCE_ROWS   = int(os.getenv("FORCE_CHUNK_ROWS", "0")) or None
    CAP_ROWS     = int(os.getenv("CHUNK_ROWS_CAP", "40000"))

    DATE_COLS    = [c.strip() for c in os.getenv("DATE_COLS","").split(",") if c.strip()]
    TIME_COLS    = [c.strip() for c in os.getenv("TIME_COLS","").split(",") if c.strip()]
    DATETIME_COLS= [c.strip() for c in os.getenv("DATETIME_COLS","").split(",") if c.strip()]
    PARTITION_BY = [c.strip() for c in os.getenv("PARTITION_BY","").split(",") if c.strip()] or None

    if not all([SMB_SERVER, SMB_SHARE, SMB_PATH, SMB_USER, SMB_PASS, BUCKET]):
        log.error("Missing required environment variables. See usage section at bottom.")
        sys.exit(2)

    s3  = s3_client()
    xfer= transfer_cfg()
    smbclient.register_session(SMB_SERVER, username=SMB_USER, password=SMB_PASS)

    manifest_key = f"{PREFIX.rstrip('/')}/{MANIFEST}"
    manifest = load_manifest(s3, BUCKET, manifest_key)
    uploaded = set(manifest.get("chunks", []))

    smb_full = f"//{SMB_SERVER}/{SMB_SHARE}/{SMB_PATH}"
    log.info(f"SMB source: {smb_full}")
    log.info(f"S3 target:  s3://{BUCKET}/{PREFIX}")

    arrow_schema = None

    def key_for_chunk(idx: int, df: pd.DataFrame) -> str:
        if PARTITION_BY:
            parts = []
            for col in PARTITION_BY:
                if col in df.columns:
                    sval = str(df[col].iloc[0]).replace("/", "-")
                else:
                    sval = "unknown"
                parts.append(f"{col}={sval}")
            part_path = "/".join(parts)
            return f"{PREFIX.rstrip('/')}/{part_path}/part-{idx:06d}.parquet"
        return f"{PREFIX.rstrip('/')}/chunk_{idx:06d}.parquet"

    def upload_chunk(idx: int, df: pd.DataFrame):
        nonlocal arrow_schema, manifest, uploaded
        key = key_for_chunk(idx, df)
        if key in uploaded:
            log.info(f"Skip existing {key}")
            return

        # Enforce stable schema and write Parquet
        tbl = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        buf = BytesIO()
        # data_page_size keeps pages predictable on very wide tables
        pq.write_table(tbl, buf, compression="snappy", use_dictionary=True, data_page_size=1<<20)
        buf.seek(0)
        s3.upload_fileobj(buf, BUCKET, key, Config=xfer)
        uploaded.add(key)
        manifest["chunks"] = sorted(list(uploaded))
        if manifest.get("schema") is None:
            manifest["schema"] = str(arrow_schema)
        save_manifest(s3, BUCKET, manifest_key, manifest)
        log.info(f"Uploaded {key} ({len(df):,} rows)")

    try:
        # ---- Gate: inspect metadata; bail on compression ----
        with smbclient.open_file(smb_full, mode="rb") as fh:
            with SAS7BDAT(fh) as r:
                row_count = getattr(r, "row_count", None)
                col_names = getattr(r, "column_names", [])
                compression = str(getattr(r, "compression", "NONE")).upper()
                platform = getattr(r, "platform", "unknown")
                encoding = getattr(r, "encoding", "unknown")
                log.info(f"Metadata: rows≈{row_count}, cols={len(col_names)}, compression={compression}, platform={platform}, encoding={encoding}")
                if compression not in ("", "NONE", "0", "FALSE", "NO"):
                    log.error("Detected compressed SAS7BDAT (COMPRESS!=NONE). This streamer does not support compressed SAS. "
                              "Use a staged local path with ReadStat/pyreadstat or SAS PROC EXPORT.")
                    sys.exit(3)

        # ---- Probe a small batch to determine schema & bytes/row ----
        with smbclient.open_file(smb_full, mode="rb") as fh:
            with SAS7BDAT(fh) as reader:
                probe_rows = []
                for _ in range(PROBE_ROWS):
                    try:
                        probe_rows.append(next(reader))
                    except StopIteration:
                        break

        if not probe_rows:
            log.warning("No rows found; writing _SUCCESS and exiting.")
            s3.put_object(Bucket=BUCKET, Key=f"{PREFIX.rstrip('/')}/_SUCCESS", Body=b"")
            return

        # Re-grab column names freshly (robustness)
        with smbclient.open_file(smb_full, mode="rb") as fh:
            with SAS7BDAT(fh) as reader:
                cols = reader.column_names

        probe_df = pd.DataFrame(probe_rows)
        if len(cols) == probe_df.shape[1]:
            probe_df.columns = cols

        probe_df = normalize_sas_dates(probe_df, DATE_COLS, TIME_COLS, DATETIME_COLS)
        probe_df = safe_utf8(probe_df)

        bpr = bytes_per_row(probe_df)
        chunk_rows = FORCE_ROWS or derive_chunk_rows(MAX_MB, bpr)
        if CAP_ROWS:
            chunk_rows = min(chunk_rows, CAP_ROWS)
        log.info(f"bytes/row≈{bpr:,.0f}; chunk_rows={chunk_rows:,} (cap={CAP_ROWS:,}, max≈{MAX_MB} MB)")

        arrow_schema = pa.Table.from_pandas(probe_df, preserve_index=False).schema

        # Emit first chunk
        upload_chunk(0, probe_df)
        emitted = len(probe_df)
        chunk_idx = 1

        del probe_df, probe_rows
        gc.collect()

        # ---- Stream remaining rows ----
        t0 = time.time()
        with smbclient.open_file(smb_full, mode="rb") as fh:
            with SAS7BDAT(fh) as reader:
                # fast-forward over the already-emitted probe rows
                skipped = 0
                while skipped < emitted:
                    try:
                        next(reader)
                        skipped += 1
                    except StopIteration:
                        break

                while True:
                    rows = []
                    for _ in range(chunk_rows):
                        try:
                            rows.append(next(reader))
                        except StopIteration:
                            break
                    if not rows:
                        break

                    df = pd.DataFrame(rows, columns=cols)
                    df = normalize_sas_dates(df, DATE_COLS, TIME_COLS, DATETIME_COLS)
                    df = safe_utf8(df)

                    upload_chunk(chunk_idx, df)
                    emitted += len(df)
                    chunk_idx += 1

                    elapsed = max(time.time() - t0, 1e-6)
                    rate = emitted / elapsed
                    log.info(f"Progress: {emitted:,} rows | {rate:,.0f} rows/s | chunks={chunk_idx}")

                    del df, rows
                    gc.collect()

        # Success marker for downstream jobs
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIX.rstrip('/')}/_SUCCESS", Body=b"")
        manifest["row_count"] = emitted
        save_manifest(s3, BUCKET, manifest_key, manifest)
        log.info("Completed successfully.")

    except Exception as e:
        log.exception(f"Failure: {e}")
        raise
    finally:
        try:
            smbclient.delete_session(SMB_SERVER)
        except Exception:
            pass


# ----------------------------- CLI -------------------------------------------

def main():
    # Optional ergonomics for pandas on wide tables
    try:
        # pandas >= 2.0
        pd.options.mode.copy_on_write = True
    except Exception:
        pass

    stream_sas_from_smb_to_s3()


if __name__ == "__main__":
    main()


# ----------------------------- Usage -----------------------------------------
"""
Environment variables (required *):
  SMB_SERVER*        e.g., "10.211.146.11"
  SMB_SHARE*         e.g., "drop place"
  SMB_FILE_PATH*     e.g., "Mehrab/Contractor/…/file.sas7bdat"
  SMB_USERNAME*      SMB user
  SMB_PASSWORD*      SMB password
  S3_BUCKET*         Target bucket
  S3_ACCESS_KEY*     Access key (or use instance/role)
  S3_SECRET_KEY*     Secret key
  S3_ENDPOINT_URL    e.g., "https://castibmcoscpt3.fg.rbc.com" for S3-compatible
  AWS_REGION         default "us-east-1"

Optional tuning:
  S3_PREFIX          default "sas_ingest/"
  MANIFEST_NAME      default "_manifest.json"
  MAX_MEMORY_MB      default "3000"
  PROBE_ROWS         default "2000"  (lower for very wide tables)
  FORCE_CHUNK_ROWS   default "0" (use adaptive); set to a number to force
  CHUNK_ROWS_CAP     default "40000" (cap for very wide tables)
  MULTIPART_CHUNK_MB default "16"
  MAX_CONCURRENCY    default "8"
  LOG_LEVEL          default "INFO"

Optional schema hints & layout:
  DATE_COLS          e.g., "as_of_date,close_dt"
  TIME_COLS          e.g., "time_of_day"
  DATETIME_COLS      e.g., "created_ts,updated_ts"
  PARTITION_BY       e.g., "ingest_date" or "year,month"

Example:
  export SMB_SERVER="10.211.146.11"
  export SMB_SHARE="drop place"
  export SMB_FILE_PATH="Mehrab/Contractor/RCL/DATA_OUT/rbc/rcl_calc_gwth0_scen5.sas7bdat"
  export SMB_USERNAME="your_ad_user"
  export SMB_PASSWORD="********"
  export S3_ACCESS_KEY="***"
  export S3_SECRET_KEY="***"
  export S3_ENDPOINT_URL="https://castibmcoscpt3.fg.rbc.com"
  export S3_BUCKET="sample-bucket-demo-12345"
  export S3_PREFIX="sas_ingest/gwth0_run_001"
  python smb_to_s3_sas_streamer.py
"""
