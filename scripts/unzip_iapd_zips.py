#!/usr/bin/env python3
"""
unzip_iapd_zips.py – Extract every ZIP file downloaded by fetch_iapd_data.py

Usage
-----
    python3 unzip_iapd_zips.py [ZIP_DIR] [OUT_DIR]

• *ZIP_DIR*  : directory that holds the .zip archives (default: ./data/raw/iapd)
• *OUT_DIR*  : where to extract `.xlsx` files        (default: ./data/unzipped/iapd)

The script:
1. Recursively walks *ZIP_DIR* for any `*.zip` file.
2. Creates *OUT_DIR* (and sub‑folders) if needed.
3. Skips extraction if the target .xlsx already exists.

Dependencies
------------
Only Python stdlib: pathlib, zipfile, tqdm (optional).

Install tqdm for a progress bar:
    pip install tqdm
"""
from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path
from typing import List

try:
    from tqdm import tqdm  # type: ignore
except ModuleNotFoundError:  # fallback – no progress bar
    tqdm = lambda x, **_: x  # noqa: E731


def find_zip_files(zip_dir: Path) -> List[Path]:
    """Return a list of all .zip files under *zip_dir* (recursive)."""
    return sorted(zip_dir.rglob("*.zip"))


def extract_zip(zip_path: Path, out_dir: Path) -> None:
    """Extract *zip_path* into *out_dir*, skipping any existing XLSX."""
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(('.xlsx', '.xls', '.csv'))]
        if not members:
            print(f"! {zip_path.name}: no Excel/CSV found – skipped")
            return

        for member in members:
            dest_path = out_dir / Path(member).name
            if dest_path.exists():
                print(f"✓ {dest_path.name} exists – skipping")
                continue
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            print(f"→ {dest_path.name} … extracting")
            with zf.open(member) as src, dest_path.open("wb") as dst:
                dst.write(src.read())


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract SEC IAPD ZIP archives")
    parser.add_argument("zip_dir", nargs="?", default="data/raw/iapd", help="Directory containing ZIP files (default: %(default)s)")
    parser.add_argument("out_dir", nargs="?", default="data/unzipped/iapd", help="Directory to place extracted files (default: %(default)s)")
    args = parser.parse_args()

    zip_dir = Path(args.zip_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    if not zip_dir.exists():
        print("ZIP directory does not exist:", zip_dir, file=sys.stderr)
        sys.exit(1)

    zip_files = find_zip_files(zip_dir)
    if not zip_files:
        print("No .zip files found in", zip_dir, file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(zip_files)} ZIP files – extracting to {out_dir}\n")

    for zf in tqdm(zip_files, desc="ZIPs", unit="file"):
        try:
            extract_zip(zf, out_dir)
        except zipfile.BadZipFile as exc:
            print(f"✗ {zf.name}: Bad ZIP ({exc}) – skipped")

    print("\nAll done ✔ – extracted files live in", out_dir)


if __name__ == "__main__":
    main()
