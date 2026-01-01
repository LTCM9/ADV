#!/bin/bash
# Docker data loading script for SEC IAPD files (local testing)
# Usage: ./load_data_docker.sh

echo "=== Loading SEC IAPD Data (Docker) ==="
echo "Step 1: Loading Excel files with 8 workers..."
docker exec -it adv-data-pipeline python3 /app/scripts/load_iapd_to_postgres.py /app/data/unzipped/iapd/ --workers 8

echo ""
echo "Step 2: Loading CSV files..."
docker exec -it adv-data-pipeline python3 /app/scripts/load_csv_files.py /app/data/unzipped/iapd/

echo ""
echo "=== Data Loading Complete ===" 