#!/usr/bin/env python3
"""
fetch_iapd_data.py – Download **Registered** Investment Adviser monthly ZIP archives.

• Scrapes https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers
• Ignores "Exempt Reporting Adviser" files by default.
• Streams each ZIP to a local directory (default: ./data/raw/iapd) and skips any already present.
• Respects DATA_FETCH_INTERVAL_HOURS from .env file to avoid unnecessary downloads.

Usage
-----
    # default output dir
    python3 fetch_iapd_data.py

    # custom destination path
    python3 fetch_iapd_data.py /path/to/dir

    # force download (ignore interval)
    python3 fetch_iapd_data.py --force

Dependencies
------------
    pip install requests beautifulsoup4 tqdm python-dotenv
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm  # progress bar
except ModuleNotFoundError:  # graceful fallback
    tqdm = lambda x, **_: x  # type: ignore  # noqa: E731

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

LISTING_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers"
)
HEADERS = {
    "User-Agent": "MyFirm-IAPDFetcher/1.0 (+mailto:ops@myfirm.com)",
}
TIMEOUT = 30  # seconds

EXCLUDE_PHRASE = "exempt reporting advisers"  # case-insensitive

# Default fetch interval (24 hours) if not specified in .env
DEFAULT_FETCH_INTERVAL_HOURS = 24


def get_fetch_interval() -> int:
    """Get fetch interval from environment variable."""
    return int(os.getenv("DATA_FETCH_INTERVAL_HOURS", DEFAULT_FETCH_INTERVAL_HOURS))


def should_fetch_data(dest_dir: Path) -> bool:
    """
    Check if we should fetch data based on the last fetch time.
    Returns True if we should fetch, False if we should skip.
    """
    fetch_interval_hours = get_fetch_interval()
    
    # Check if any files exist in the destination directory
    if not dest_dir.exists() or not any(dest_dir.glob("*.zip")):
        print(f"No existing files found in {dest_dir} - will download")
        return True
    
    # Find the most recent ZIP file
    zip_files = list(dest_dir.glob("*.zip"))
    if not zip_files:
        print(f"No ZIP files found in {dest_dir} - will download")
        return True
    
    # Get the most recent file's modification time
    latest_file = max(zip_files, key=lambda f: f.stat().st_mtime)
    latest_time = datetime.fromtimestamp(latest_file.stat().st_mtime)
    current_time = datetime.now()
    
    # Calculate time difference
    time_diff = current_time - latest_time
    hours_since_last_fetch = time_diff.total_seconds() / 3600
    
    if hours_since_last_fetch >= fetch_interval_hours:
        print(f"Last fetch was {hours_since_last_fetch:.1f} hours ago (threshold: {fetch_interval_hours} hours) - will download")
        return True
    else:
        print(f"Last fetch was {hours_since_last_fetch:.1f} hours ago (threshold: {fetch_interval_hours} hours) - skipping download")
        print(f"Next fetch available in {fetch_interval_hours - hours_since_last_fetch:.1f} hours")
        return False


def discover_zip_urls() -> List[str]:
    """Return absolute URLs to ZIP files **excluding** Exempt Reporting Adviser files and only from 2020 onwards."""
    r = requests.get(LISTING_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    zip_links: List[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.lower().endswith(".zip"):
            continue
        label = a.get_text(strip=True).lower()
        if EXCLUDE_PHRASE in label:
            continue  # skip ERA datasets
        
        # Extract year from filename (e.g., ia010120.zip -> 2020)
        filename = href.split("/")[-1]
        year = extract_year_from_filename(filename)
        if year is None or year < 2020:
            continue  # skip files before 2020
            
        url = href if href.startswith("http") else f"https://www.sec.gov{href}"
        zip_links.append(url)

    if not zip_links:
        raise RuntimeError(
            "No Registered IA ZIP links found from 2020 onwards – the page layout or filtering may have changed."
        )
    return zip_links


def extract_year_from_filename(filename: str) -> Optional[int]:
    """Extract year from filename like 'ia010120.zip' -> 2020, 'ia-050324.zip' -> 2024, 'ia020119-2.zip' -> 2019"""
    import re
    # Pattern: iaMMDDYY.zip, iaMMDDYYYY.zip, ia-MMDDYY.zip, iaMMDDYY-2.zip, iaMMDDYY_2.zip
    match = re.search(r'ia-?(\d{6,8})[-_]?\d*\.zip', filename)
    if match:
        date_str = match.group(1)
        if len(date_str) == 6:  # MMDDYY format
            year = int('20' + date_str[4:6])
        else:  # MMDDYYYY format
            year = int(date_str[4:8])
        return year
    return None


def download(url: str, dest: Path) -> None:
    """Stream *url* into *dest* unless the file already exists."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        print(f"✓ {dest.name} exists – skipping")
        return

    with requests.get(url, stream=True, headers=HEADERS, timeout=TIMEOUT) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        print(f"↓ {dest.name} …")
        with dest.open("wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=dest.name,
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 32):
                f.write(chunk)
                bar.update(len(chunk))


def main():
    parser = argparse.ArgumentParser(description="Fetch SEC Registered IA ZIP archives")
    parser.add_argument(
        "dest_dir",
        nargs="?",
        default="data/raw/iapd",
        help="Directory to store ZIP files (default: %(default)s)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force download even if interval hasn't passed",
    )
    args = parser.parse_args()
    dest_dir = Path(args.dest_dir).expanduser().resolve()

    # Check if we should fetch data based on interval
    if not args.force and not should_fetch_data(dest_dir):
        print("Use --force to override the fetch interval")
        return

    try:
        urls = discover_zip_urls()
    except Exception as exc:
        print("Error discovering ZIP URLs:", exc, file=sys.stderr)
        sys.exit(1)

    print(f"Discovered {len(urls)} Registered IA ZIP files → {dest_dir}\n")

    for url in urls:
        filename = url.split("/")[-1]
        try:
            download(url, dest_dir / filename)
        except Exception as exc:  # noqa: BLE001
            print(f"✗ {filename}: {exc}")
            time.sleep(2)
            continue

    print("\nAll done ✔ – Registered IA ZIPs are ready in", dest_dir)


if __name__ == "__main__":
    main()