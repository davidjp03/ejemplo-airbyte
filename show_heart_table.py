"""
Heart Dataset Visualization Tool

This script displays transformed heart dataset from S3 Parquet files:
- Shows key health metrics and transformations
- Displays sleep patterns and health risk categories
- Provides summary statistics for the heart dataset

Usage:
- python show_heart_table.py heart_transformed_001.parquet
"""

import pandas as pd
import boto3
import os
import sys
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

def show_heart_table(parquet_filename):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    
    bucket = os.getenv("S3_BUCKET_NAME")
    key = f"{os.getenv('S3_BUCKET_PATH')}heart/{parquet_filename}"
    
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    
    print(f"HEART DATASET RESULTS FOR: {parquet_filename}")
    print("=" * 80)
    selected_cols = ['edad', 'sexo', 'horas_sueno', 'salud_mental', 'salud_fisica', 'sleep_category', 'health_risk']
    print(df[selected_cols].head(10).to_string(index=False))
    
    print(f"\nSUMMARY:")
    print(f"Total records: {len(df)}")
    print(f"Sleep categories: {df['sleep_category'].value_counts().to_dict()}")
    print(f"Health risk levels: {df['health_risk'].value_counts().to_dict()}")
    print(f"Average sleep hours: {df['horas_sueno'].mean():.2f}")
    print(f"Average mental health score: {df['salud_mental'].mean():.2f}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        show_heart_table(sys.argv[1])
    else:
        show_heart_table("heart_transformed_001.parquet")