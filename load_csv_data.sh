#!/bin/bash
# CSV data loading script for SEC IAPD files
# Usage: ./load_csv_data.sh

echo "Loading SEC IAPD CSV files..."
docker exec -it adv-data-pipeline python3 /app/scripts/load_csv_files.py /app/data/unzipped/iapd/ 