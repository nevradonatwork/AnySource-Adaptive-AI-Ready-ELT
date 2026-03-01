"""
main.py

Entry point: run the full ETL pipeline for an existing domain.

Usage:
    python main.py --domain <domain_name> --partition <partition_value>

Examples:
    python main.py --domain sales_europe --partition 2025-01
    python main.py --domain inventory     --partition 2025-W04
    python main.py --domain daily_metrics --partition 2025-01-15
"""

import argparse
import sys

from core.pipeline import run_pipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the AnySource ELT pipeline for a domain and partition."
    )
    parser.add_argument(
        "--domain",
        required=True,
        help="Domain name (e.g. sales_europe)",
    )
    parser.add_argument(
        "--partition",
        required=True,
        help=(
            "Partition value. Format depends on PARTITION_GRANULARITY: "
            "monthly=YYYY-MM, weekly=YYYY-Www, daily=YYYY-MM-DD"
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        run_pipeline(domain_name=args.domain, partition_value=args.partition)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
