#!/usr/bin/env python3
import pandas as pd
import sqlalchemy as sa

# Test database connection
engine = sa.create_engine('postgresql://postgres:password@localhost:5432/adv_db')
print('Testing connection...')
with engine.connect() as conn:
    print('Connection successful')

# Test reading a file
print('Reading test file...')
df = pd.read_excel('data/unzipped/iapd/ia040117.xlsx', nrows=10)
print(f'Read {len(df)} rows from file')
print(f'Columns: {list(df.columns)[:5]}...')

# Test normalization
from scripts.load_iapd_basic import normalize_dataframe
normalized = normalize_dataframe(df)
print(f'Normalized columns: {list(normalized.columns)}')
print(f'CRD values: {normalized["crd"].head().tolist() if "crd" in normalized.columns else "No CRD"}')

# Test database load
from scripts.load_iapd_basic import load_data_to_db
result = load_data_to_db(normalized, engine)
print(f'Loaded {result} rows to database') 