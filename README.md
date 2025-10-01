# PyAirbyte ELT Pipeline with S3

Complete ELT (Extract-Load-Transform) pipeline using PyAirbyte and AWS S3 for data processing.

## Architecture

```
Source (Faker) → S3 (Raw Data) → Transform → S3 (Processed Data)
```

## Files

- **`elt_airbyte_direct_s3.py`** - Extract & Load pipeline (Faker → S3)
- **`transform_s3_pandas.py`** - Transform pipeline (S3 → Transform → S3)
- **`show_table.py`** - Display transformed data from any Parquet file
- **`verify_transformations.py`** - Legacy verification script

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables in `.env`:
```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket
S3_BUCKET_PATH=airbyte_elt/
```

## Usage

### 1. Extract & Load Data
```bash
python elt_airbyte_direct_s3.py
```
Generates sample data and loads to S3 as `products.parquet`

### 2. Transform Data
```bash
python transform_s3_pandas.py
```
Applies transformations and saves as `products_transformed_001.parquet` (serial numbered)

### 3. View Results
```bash
python show_table.py products_transformed_001.parquet
```

## Transformations

- **price_category**: luxury (>$50k), premium (>$20k), standard (≤$20k)
- **created_date**: Date extracted from created_at timestamp

## S3 Structure

```
s3://your-bucket/airbyte_elt/products/
├── products.parquet                    # Raw data
├── products_transformed_001.parquet    # Transformed data
├── products_transformed_002.parquet    # Next transformation
└── ...
```