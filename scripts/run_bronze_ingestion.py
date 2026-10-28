"""Run one metadata-driven Bronze ingestion configuration or selected source from central config."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from accelerator.bronze_ingestion import ingest  # noqa: E402
from accelerator.config_loader import load_central_config  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Land a configured source in local Bronze JSONL storage."
    )
    parser.add_argument(
        "positional_config",
        nargs="?",
        default=None,
        help="Path to source JSON configuration (legacy positional interface)",
    )
    parser.add_argument(
        "--config",
        "-c",
        dest="config_path",
        default=None,
        help="Path to centralYAML (e.g. config.yml) or source JSON configuration file",
    )
    parser.add_argument(
        "--source",
        "-s",
        dest="source_name",
        default=None,
        help="Name of the source dataset to ingest (when using central YAML configuration)",
    )
    parser.add_argument(
        "--submission-key",
        "-k",
        dest="submission_key",
        default=None,
        help="Optional client-supplied submission key for idempotent retry and recovery",
    )
    args = parser.parse_args()

    config_file = args.config_path or args.positional_config
    if not config_file:
        parser.error("A configuration path must be provided via argument or --config.")

    resolved_path = Path(config_file)
    if not resolved_path.exists():
        sys.exit(f"Error: Configuration file '{config_file}' does not exist.")

    if resolved_path.suffix.lower() in (".yml", ".yaml"):
        if not args.source_name:
            sys.exit(
                f"Error: Using central configuration '{config_file}' requires specifying --source."
            )
        try:
            central = load_central_config(resolved_path)
            source_cfg = central.get_source(args.source_name)
        except Exception as err:
            sys.exit(f"Configuration error: {err}")

        result = ingest(source_cfg, submission_key=args.submission_key)
    else:
        if args.source_name:
            print(
                f"[INFO] Direct JSON configuration provided; ignoring --source '{args.source_name}'.",
                file=sys.stderr,
            )
        try:
            result = ingest(resolved_path, submission_key=args.submission_key)
        except Exception as err:
            sys.exit(f"Ingestion error: {err}")

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
