"""
Transformation Verification Script

This script verifies that transformations were applied correctly:
- Reads the default transformed Parquet file from S3
- Displays sample data with transformation columns
- Shows basic statistics about the transformed dataset

Note: This is a legacy verification script. Use show_table.py for better formatting.
"""

import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Read transformed data
bucket = os.getenv("S3_BUCKET_NAME")
key = f"{os.getenv('S3_BUCKET_PATH')}products/products_transformed.parquet"

obj = s3_client.get_object(Bucket=bucket, Key=key)
df = pd.read_parquet(BytesIO(obj['Body'].read()))

print("Transformed data sample:")
print(df[['make', 'model', 'year', 'price', 'price_category', 'car_age', 'created_date']].head())
print(f"\nTotal records: {len(df)}")
print(f"New columns: {[col for col in df.columns if col in ['price_category', 'car_age', 'created_date']]}")