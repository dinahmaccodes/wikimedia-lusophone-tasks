#!/usr/bin/env python3
"""Read URLs from a CSV file and print each response status code."""

from __future__ import annotations

import argparse
import csv
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def iter_urls(csv_path: Path) -> Iterator[str]:
    """Yield non-empty URL values from the CSV file."""
    with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if not reader.fieldnames:
            return

        url_field = "urls" if "urls" in reader.fieldnames else reader.fieldnames[0]

        for row in reader:
            value = (row.get(url_field) or "").strip()
            if value:
                yield value


def fetch_status_code(url: str, timeout: float) -> str:
    """Return the status code for a URL request, or an error label.

    This script is a quick dataset-audit tool: its job is to show which URLs
    still respond so we can decide whether the source list needs cleanup,
    redirects, or a retry strategy later.
    """
    # HEAD avoids downloading page bodies, which keeps the audit faster and
    # lighter on bandwidth. Some servers reject HEAD, so we fall back to GET.
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"}, method="HEAD")
    try:
        with urlopen(request, timeout=timeout) as response:
            return str(response.status)
    except HTTPError as error:
        if error.code in {405, 501}:
            fallback_request = Request(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                method="GET",
            )
            with urlopen(fallback_request, timeout=timeout) as response:
                return str(response.status)
        return str(error.code)
    except TimeoutError:
        return "TIMEOUT"
    except URLError as error:
        reason = getattr(error, "reason", None)
        if isinstance(reason, TimeoutError):
            return "TIMEOUT"
        if reason is not None:
            return reason.__class__.__name__.upper()
        return "ERROR"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print the status code of each URL from a CSV file."
    )
    parser.add_argument(
        "csv_file",
        nargs="?",
        default=Path(__file__).with_name("Task 2 - Intern.csv"),
        help="Path to the CSV file containing URLs (default: the CSV beside this script)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=8.0,
        help="Request timeout in seconds (default: 8)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of URLs to fetch in parallel (default: 8)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv_file)

    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    print("url,status_code")
    urls = list(iter_urls(csv_path))
    # Run the checks in parallel so slow or dead links do not stall the whole audit.
    if not urls:
        return

    with ThreadPoolExecutor(max_workers=min(args.workers, len(urls))) as executor:
        status_fetcher = partial(fetch_status_code, timeout=args.timeout)
        for url, status_code in zip(urls, executor.map(status_fetcher, urls)):
            print(f"{url},{status_code}")


if __name__ == "__main__":
    main()
