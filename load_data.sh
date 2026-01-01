#!/bin/bash
# Data loading script for SEC IAPD files
# Usage: ./load_data.sh

echo "=== Loading SEC IAPD Data ==="
echo "Step 1: Loading Excel files with 8 workers..."
python3 scripts/load_iapd_to_postgres.py data/unzipped/iapd/ --workers 8

echo ""
echo "Step 2: Loading CSV files..."
python3 scripts/load_csv_files.py data/unzipped/iapd/

echo ""
echo "=== Data Loading Complete ===" 