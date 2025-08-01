#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Set environment variables
os.environ["PGHOST"] = "localhost"
os.environ["PGPORT"] = "5432"
os.environ["PGDATABASE"] = "iapd"
os.environ["PGUSER"] = "iapdadmin"
os.environ["PGPASSWORD"] = "AdvPwd#2025"

# Import and run the load script
sys.path.append(str(Path(__file__).parent))
from scripts.load_iapd_to_postgres import main

if __name__ == "__main__":
    main() 