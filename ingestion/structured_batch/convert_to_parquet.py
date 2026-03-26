import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()

client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
)

bucket = os.getenv("MINIO_BUCKET", "lakehouse")

raw_object = "bronze/temporal/lastfm/run_date=2026-03-26/run_id=20260326T150000Z/lastfm_tracks_raw.csv"
parquet_object = "bronze/persistent/lastfm/run_date=2026-03-26/lastfm_tracks_raw.parquet"

# Read CSV from MinIO
response = client.get_object(bucket, raw_object)
df = pd.read_csv(response)
response.close()
response.release_conn()

# Optional type cleaning
df["lastfm_duration"] = pd.to_numeric(df["lastfm_duration"], errors="coerce")
df["lastfm_listeners"] = pd.to_numeric(df["lastfm_listeners"], errors="coerce")
df["lastfm_playcount"] = pd.to_numeric(df["lastfm_playcount"], errors="coerce")
df["source_page"] = pd.to_numeric(df["source_page"], errors="coerce")

# Convert to Parquet in memory
table = pa.Table.from_pandas(df)
buffer = io.BytesIO()
pq.write_table(table, buffer)
buffer.seek(0)

client.put_object(
    bucket,
    parquet_object,
    data=buffer,
    length=buffer.getbuffer().nbytes,
    content_type="application/octet-stream"
)

print(f"Uploaded parquet to s3://{bucket}/{parquet_object}")