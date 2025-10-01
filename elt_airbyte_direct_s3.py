"""
ELT Pipeline - Extract and Load Phase

This script performs the EL (Extract-Load) part of an ELT pipeline:
1. Extracts data from PyAirbyte's source-faker connector
2. Loads raw data directly to S3 as Parquet format

No transformations are applied - data is stored in its original form.
"""

import airbyte as ab
import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

# 1. Source: Faker (genera datos ficticios)
source = ab.get_source("source-faker")
source.set_config({
    "count": 500_000,
    "seed": 123
})
source.check()

# 2. Configurar cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# 3. ELT - Leer datos y escribir a S3
source.select_all_streams()
df = source.read()
df_products = df["products"]

# Convertir a DataFrame de pandas
products_df = df_products.to_pandas()

# Escribir a S3 como Parquet
buffer = BytesIO()
products_df.to_parquet(buffer, compression='snappy', index=False)
buffer.seek(0)

# Subir a S3
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_key = f"{os.getenv('S3_BUCKET_PATH')}products/products.parquet"

s3_client.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=buffer.getvalue(),
    ContentType='application/octet-stream'
)

print(f"Datos cargados en S3: s3://{bucket_name}/{s3_key}")
print(f"Total de registros: {len(products_df)}")