"""
Data Visualization Tool

This script provides a function to display transformed data from S3 Parquet files:
- Reads specified Parquet file from S3
- Displays formatted table with key columns
- Shows summary statistics including record count and price categories

Usage:
- python show_table.py filename.parquet  # Show specific file
- python show_table.py                    # Show default file
"""

import pandas as pd
import boto3
import os
import sys
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

def show_table(parquet_filename):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    
    bucket = os.getenv("S3_BUCKET_NAME")
    key = f"{os.getenv('S3_BUCKET_PATH')}products/{parquet_filename}"
    
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    
    print(f"RESULTS FOR: {parquet_filename}")
    print("=" * 80)
    selected_cols = ['make', 'model', 'year', 'price', 'price_category', 'created_date']
    print(df[selected_cols].head(10).to_string(index=False))
    
    print(f"\nSUMMARY:")
    print(f"Total records: {len(df)}")
    print(f"Price categories: {df['price_category'].value_counts().to_dict()}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        show_table(sys.argv[1])
    else:
        show_table("products_transformed.parquet")
