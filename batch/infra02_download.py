"""
INFRA-02: GitHub Archive Bulk Download
GitHub Developer Ecosystem Analytics
Author: Hariharan L (hl5865)

Downloads GitHub Archive JSON.GZ files for 2023-2025 from GCS using gsutil,
then uploads to HDFS for batch processing by INFRA-04.

Usage:
    # Full 3-year download (~120GB) — runs in background
    python infra02_download.py

    # Specific year only
    python infra02_download.py --years 2025

    # Sample mode (Jan-Mar 2025 only, ~10GB) for local dev
    python infra02_download.py --sample

Output:
    Local:  data/raw/YYYY-MM-DD-HH.json.gz
    HDFS:   /github/events/raw/YYYY-MM-DD-HH.json.gz
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

GCS_BASE = "gs://data.gharchive.org"
LOCAL_RAW = Path("data/raw")
HDFS_RAW = "/github/events/raw"

FULL_YEARS = ["2023", "2024", "2025"]
SAMPLE_MONTHS = ["2025-01", "2025-02", "2025-03"]


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    log.info("$ %s", " ".join(cmd))
    return subprocess.run(cmd, check=check)


def download_gcs(pattern: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    run(["gsutil", "-m", "cp", "-n", pattern, str(dest)])


def upload_hdfs(src: Path) -> None:
    run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", HDFS_RAW], check=False)
    for gz in sorted(src.glob("*.json.gz")):
        run([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-put", "-f",
            f"/tmp/raw/{gz.name}", f"{HDFS_RAW}/{gz.name}",
        ], check=False)


def copy_to_namenode(src: Path) -> None:
    run(["docker", "cp", str(src), "namenode:/tmp/raw"])


def download_years(years: list[str]) -> None:
    for year in years:
        pattern = f"{GCS_BASE}/{year}-*.json.gz"
        log.info("Downloading year %s ...", year)
        download_gcs(pattern, LOCAL_RAW)
    log.info("GCS download complete. Files in %s", LOCAL_RAW)


def download_sample() -> None:
    for month in SAMPLE_MONTHS:
        pattern = f"{GCS_BASE}/{month}-*.json.gz"
        log.info("Downloading sample month %s ...", month)
        download_gcs(pattern, LOCAL_RAW)
    log.info("Sample download complete. Files in %s", LOCAL_RAW)


def push_to_hdfs() -> None:
    log.info("Copying files to namenode container ...")
    copy_to_namenode(LOCAL_RAW)
    log.info("Uploading to HDFS %s ...", HDFS_RAW)
    upload_hdfs(LOCAL_RAW)
    log.info("HDFS upload complete.")


def verify_hdfs() -> None:
    log.info("Verifying HDFS contents ...")
    run(["docker", "exec", "namenode", "hdfs", "dfs", "-ls", HDFS_RAW])


def main() -> None:
    parser = argparse.ArgumentParser(description="INFRA-02: GitHub Archive downloader")
    parser.add_argument(
        "--years", nargs="+", default=FULL_YEARS,
        help="Years to download (default: 2023 2024 2025)",
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="Download Jan-Mar 2025 only (~10GB, for local dev)",
    )
    parser.add_argument(
        "--skip-hdfs", action="store_true",
        help="Skip HDFS upload (download to data/raw only)",
    )
    args = parser.parse_args()

    if args.sample:
        download_sample()
    else:
        download_years(args.years)

    if not args.skip_hdfs:
        push_to_hdfs()
        verify_hdfs()

    local_files = list(LOCAL_RAW.glob("*.json.gz"))
    log.info("Done. %d files in %s", len(local_files), LOCAL_RAW)


if __name__ == "__main__":
    main()
