#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# Test reading one CSV file
csv_file = Path("data/unzipped/iapd/ia010322.csv")

print(f"Testing CSV file: {csv_file}")
print(f"File exists: {csv_file.exists()}")

# Try different approaches
approaches = [
    ("Simple comma", {"sep": ","}),
    ("Simple pipe", {"sep": "|"}),
    ("Comma with latin-1", {"sep": ",", "encoding": "latin1"}),
    ("Pipe with latin-1", {"sep": "|", "encoding": "latin1"}),
    ("Auto-detect", {"sep": None, "encoding": "latin1"}),
]

for name, params in approaches:
    try:
        print(f"\nTrying {name}...")
        df = pd.read_csv(csv_file, **params)
        print(f"✅ Success with {name}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)[:5]}...")
        print(f"First few rows:")
        print(df.head(2))
        break
    except Exception as e:
        print(f"❌ Failed with {name}: {str(e)[:100]}...") 