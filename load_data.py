#!/usr/bin/env python3
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed, using environment variables")

# Import and run the load script
sys.path.append(str(Path(__file__).parent))
from scripts.load_iapd_to_postgres import main

if __name__ == "__main__":
    main()   