"""Run one metadata-driven Bronze ingestion configuration."""

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from accelerator.bronze_ingestion import ingest  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Land a configured source in local Bronze JSONL storage.")
    parser.add_argument("config", help="Path to source JSON configuration")
    args = parser.parse_args()
    print(json.dumps(ingest(args.config), indent=2))


if __name__ == "__main__":
    main()
