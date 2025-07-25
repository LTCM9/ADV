#!/usr/bin/env python3
"""
fetch_iapd_data.py – Download **Registered** Investment Adviser monthly ZIP archives.

• Scrapes https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers
• Ignores "Exempt Reporting Adviser" files by default.
• Streams each ZIP to a local directory (default: ./data/raw/iapd) and skips any already present.

Usage
-----
    # default output dir
    python3 fetch_iapd_data.py

    # custom destination path
    python3 fetch_iapd_data.py /path/to/dir

Dependencies
------------
    pip install requests beautifulsoup4 tqdm
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import List

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm  # progress bar
except ModuleNotFoundError:  # graceful fallback
    tqdm = lambda x, **_: x  # type: ignore  # noqa: E731

LISTING_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers"
)
HEADERS = {
    "User-Agent": "MyFirm-IAPDFetcher/1.0 (+mailto:ops@myfirm.com)",
}
TIMEOUT = 30  # seconds


EXCLUDE_PHRASE = "exempt reporting advisers"  # case-insensitive


def discover_zip_urls() -> List[str]:
    """Return absolute URLs to ZIP files **excluding** Exempt Reporting Adviser files."""
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
        url = href if href.startswith("http") else f"https://www.sec.gov{href}"
        zip_links.append(url)

    if not zip_links:
        raise RuntimeError(
            "No Registered IA ZIP links found – the page layout or filtering may have changed."
        )
    return zip_links


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
    args = parser.parse_args()
    dest_dir = Path(args.dest_dir).expanduser().resolve()

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