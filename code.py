import pandas as pd
import boto3
import tempfile
import os
from sas7bdat import SAS7BDAT
from io import BytesIO
import gc
from botocore.exceptions import ClientError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def stream_sas_to_s3_parquet(sas_file_path, s3_bucket, s3_prefix, 
                            chunk_size=10000, max_memory_mb=3500):
    """
    Stream SAS7BDAT file from NAS to S3 as partitioned Parquet files
    with strict memory management.
    
    Args:
        sas_file_path (str): Path to SAS file on NAS
        s3_bucket (str): S3 bucket name
        s3_prefix (str): S3 prefix/folder for output
        chunk_size (int): Rows per chunk (adjust based on column count)
        max_memory_mb (int): Memory limit in MB to stay under
    """
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Get file metadata first
    with SAS7BDAT(sas_file_path) as reader:
        total_rows = reader.row_count
        columns = reader.column_names
        logger.info(f"File has {total_rows:,} rows and {len(columns)} columns")
    
    # Calculate optimal chunk size based on memory constraints
    # Estimate memory per row (conservative estimate)
    estimated_row_size = 500  # bytes per row (conservative)
    safe_chunk_size = min(chunk_size, (max_memory_mb * 1024 * 1024) // estimated_row_size)
    logger.info(f"Using chunk size: {safe_chunk_size} rows")
    
    # Process file in chunks
    chunk_number = 0
    rows_processed = 0
    
    with SAS7BDAT(sas_file_path) as reader:
        while rows_processed < total_rows:
            try:
                # Read chunk
                chunk_data = reader.read_many(safe_chunk_size)
                if not chunk_data:
                    break
                
                # Convert to DataFrame
                df_chunk = pd.DataFrame(chunk_data, columns=columns)
                rows_processed += len(df_chunk)
                
                # Write to temporary parquet file
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    df_chunk.to_parquet(tmp_file.name, index=False, compression='snappy')
                    
                    # Upload to S3
                    s3_key = f"{s3_prefix}/chunk_{chunk_number:06d}.parquet"
                    try:
                        s3_client.upload_file(tmp_file.name, s3_bucket, s3_key)
                        logger.info(f"Uploaded chunk {chunk_number} with {len(df_chunk):,} rows to s3://{s3_bucket}/{s3_key}")
                    except ClientError as e:
                        logger.error(f"Failed to upload chunk {chunk_number}: {e}")
                        # Implement retry logic here if needed
                
                # Clean up
                os.unlink(tmp_file.name)
                del df_chunk, chunk_data
                gc.collect()  # Force garbage collection
                
                chunk_number += 1
                
                # Progress update
                if rows_processed % (safe_chunk_size * 10) == 0:
                    logger.info(f"Progress: {rows_processed:,}/{total_rows:,} rows ({rows_processed/total_rows*100:.1f}%)")
                    
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_number}: {e}")
                # Implement retry logic or checkpointing here
                break
    
    logger.info(f"Processing complete! {rows_processed:,} rows processed in {chunk_number} chunks")

# Example usage
if __name__ == "__main__":
    # Configuration
    sas_file_path = "/nas/path/to/large_file.sas7bdat"
    s3_bucket = "your-bucket-name"
    s3_prefix = "parquet_output/date=20230101"
    
    # Process the file
    stream_sas_to_s3_parquet(
        sas_file_path=sas_file_path,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        chunk_size=25000,  # Start with this, adjust based on memory
        max_memory_mb=3500  # Leave some buffer for system processes
    )
