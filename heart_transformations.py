"""
Heart Dataset Transformation Pipeline

This script transforms heart/health dataset from S3:
1. Reads raw heart dataset from S3 Parquet files
2. Applies sleep hours normalization transformations
3. Saves transformed data back to S3 with serial numbering

Transformations applied:
- Sleep hours normalization based on health conditions
- Data quality improvements for outlier values
"""

import pandas as pd
import numpy as np
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
key = f"{os.getenv('S3_BUCKET_PATH')}corazon_dataset.csv/2025_10_01_1759337927006_0.parquet"

obj = s3_client.get_object(Bucket=bucket, Key=key)
df = pd.read_parquet(BytesIO(obj['Body'].read()))

# Inspect data structure
print("Dataset info:")
print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"Sleep column type: {df['horas_sueno'].dtype}")
print(f"Sample values: {df['horas_sueno'].head()}")

# Convert numeric columns to proper data types
df['horas_sueno'] = pd.to_numeric(df['horas_sueno'], errors='coerce')
df['salud_mental'] = pd.to_numeric(df['salud_mental'], errors='coerce')
df['salud_fisica'] = pd.to_numeric(df['salud_fisica'], errors='coerce')

# Transformations
# Calculate average sleep hours for normal range (6-14 hours)
df_promedio_horas = df[(df.horas_sueno >= 6) & (df.horas_sueno <= 14)]
promedio_horas = df_promedio_horas['horas_sueno'].mean()

# Normalize sleep hours for outliers with poor mental health
df["horas_sueno"] = np.where(
    ((df['horas_sueno'] < 6) | (df['horas_sueno'] > 14)) & (df.salud_mental < 15), 
    promedio_horas, 
    df['horas_sueno']
)

# Add transformation metadata
df['sleep_category'] = df['horas_sueno'].apply(
    lambda x: 'insufficient' if x < 6 else 'excessive' if x > 9 else 'normal'
)
df['health_risk'] = df.apply(
    lambda row: 'high' if row['salud_mental'] < 15 and row['horas_sueno'] < 6 else 'low', 
    axis=1
)

# Write back to S3 with serial numbering
buffer = BytesIO()
df.to_parquet(buffer, compression='snappy', index=False)
buffer.seek(0)

# Get next serial number
response = s3_client.list_objects_v2(
    Bucket=bucket,
    Prefix=f"{os.getenv('S3_BUCKET_PATH')}heart/heart_transformed_"
)
serial = len(response.get('Contents', [])) + 1
output_key = f"{os.getenv('S3_BUCKET_PATH')}heart/heart_transformed_{serial:03d}.parquet"

s3_client.put_object(
    Bucket=bucket,
    Key=output_key,
    Body=buffer.getvalue(),
    ContentType='application/octet-stream'
)

print(f"Transformed heart data saved to: s3://{bucket}/{output_key}")
print(f"Added columns: sleep_category, health_risk")
print(f"Total records: {len(df)}")
print(f"Average normalized sleep hours: {df['horas_sueno'].mean():.2f}")