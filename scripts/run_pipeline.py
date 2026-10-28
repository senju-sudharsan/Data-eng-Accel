"""Run an end-to-end Bronze-Silver data pipeline."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from accelerator.config_loader import load_central_config  # noqa: E402
from accelerator.pipeline_runner import run_pipeline  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Execute an end-to-end Bronze-Silver data pipeline.'
    )
    parser.add_argument(
        'positional_config',
        nargs='?',
        default=None,
        help='Path to pipeline JSON configuration (legacy positional interface)',
    )
    parser.add_argument(
        '--config',
        '-c',
        dest='config_path',
        default=None,
        help='Path to central YAML (e.g. config.yml) or pipeline JSON configuration file',
    )
    parser.add_argument(
        '--pipeline',
        '-p',
        dest='pipeline_name',
        default=None,
        help='Name of the pipeline to execute (when using central YAML configuration)',
    )
    parser.add_argument(
        '--submission-key',
        '-k',
        dest='submission_key',
        default=None,
        help='Optional client-supplied submission key for idempotent retry and deduplication',
    )
    args = parser.parse_args()

    config_file = args.config_path or args.positional_config
    if not config_file:
        parser.error('A configuration path must be provided via argument or --config.')

    resolved_path = Path(config_file)
    if not resolved_path.exists():
        sys.exit(f'Error: Configuration file {config_file!r} does not exist.')

    if resolved_path.suffix.lower() in ('.yml', '.yaml'):
        if not args.pipeline_name:
            sys.exit(
                f'Error: Using central configuration {config_file!r} requires specifying --pipeline.'
            )
        try:
            central = load_central_config(resolved_path)
            pipeline_cfg = central.get_pipeline(args.pipeline_name)
        except Exception as err:
            sys.exit(f'Configuration error: {err}')

        summary = run_pipeline(pipeline_cfg, submission_key=args.submission_key)
    else:
        if args.pipeline_name:
            print(
                f'[INFO] Direct JSON configuration provided; ignoring --pipeline {args.pipeline_name!r}.',
                file=sys.stderr,
            )
        try:
            summary = run_pipeline(resolved_path, submission_key=args.submission_key)
        except Exception as err:
            sys.exit(f'Pipeline execution error: {err}')

    print(json.dumps(dataclasses.asdict(summary), indent=2, default=str))


if __name__ == '__main__':
    main()
