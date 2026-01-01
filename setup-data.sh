#!/bin/bash

echo "🚀 Setting up ADV Data Pipeline with Docker..."

# Create data directory if it doesn't exist
mkdir -p data/raw/iapd

# Start PostgreSQL and data pipeline containers
echo "📦 Starting Docker containers..."
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 10

# Check if PostgreSQL is ready
until docker-compose exec -T postgres pg_isready -U iapdadmin -d iapd; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

echo "✅ PostgreSQL is ready!"

# Run the data pipeline
echo "📊 Running data pipeline..."

# Step 1: Fetch IAPD data
echo "📥 Fetching IAPD data..."
docker-compose exec data-pipeline python scripts/fetch_iapd_data.py

# Step 2: Unzip the data (if needed)
echo "📂 Extracting data..."
docker-compose exec data-pipeline python scripts/unzip_iapd_zips.py

# Step 3: Load data into PostgreSQL
echo "💾 Loading data into PostgreSQL..."
docker-compose exec data-pipeline python scripts/load_iapd_to_postgres.py data/raw/iapd/extracted

# Step 4: Calculate risk scores
echo "🎯 Calculating risk scores..."
docker-compose exec data-pipeline python scripts/run_risk_calculation.py

echo "✅ Data pipeline complete!"
echo "🌐 You can now access the database at localhost:5432"
echo "📊 Database: iapd, User: iapdadmin, Password: AdvPwd#2025" 