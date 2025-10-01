"""
ELT Pipeline - Transform Phase

This script performs the T (Transform) part of an ELT pipeline:
1. Reads raw data from S3 Parquet files
2. Applies business transformations (price categorization, date extraction)
3. Saves transformed data back to S3 with serial numbering

Transformations applied:
- price_category: Categorizes cars as luxury/premium/standard based on price
- created_date: Extracts date from created_at timestamp
"""

import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

# S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Read from S3
bucket = os.getenv("S3_BUCKET_NAME")
key = f"{os.getenv('S3_BUCKET_PATH')}products/products.parquet"

obj = s3_client.get_object(Bucket=bucket, Key=key)
df = pd.read_parquet(BytesIO(obj['Body'].read()))

# Transformations
df['price_category'] = df['price'].apply(
    lambda x: 'luxury' if x > 50000 else 'premium' if x > 20000 else 'standard'
)

df['created_date'] = pd.to_datetime(df['created_at']).dt.date

# Write back to S3
buffer = BytesIO()
df.to_parquet(buffer, compression='snappy', index=False)
buffer.seek(0)

# Get next serial number
response = s3_client.list_objects_v2(
    Bucket=bucket,
    Prefix=f"{os.getenv('S3_BUCKET_PATH')}products/products_transformed_"
)
serial = len(response.get('Contents', [])) + 1
output_key = f"{os.getenv('S3_BUCKET_PATH')}products/products_transformed_{serial:03d}.parquet"
s3_client.put_object(
    Bucket=bucket,
    Key=output_key,
    Body=buffer.getvalue(),
    ContentType='application/octet-stream'
)

print(f"Transformed data saved to: s3://{bucket}/{output_key}")
print(f"Added columns: price_category, created_date")