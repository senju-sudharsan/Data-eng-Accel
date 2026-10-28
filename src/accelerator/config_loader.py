"""Central configuration loader and schema validator for the Data Engineering Accelerator.

Supports root-level `config.yml` with:
- Global path defaults and metadata store configuration.
- Multiple named sources (CSV, JSON array, NDJSON) with encoding, delimiter, and path resolution.
- Multiple named pipelines (Silver transformation, quality rules, SCD2 tracking, audit).
- Validation against unknown or misspelled keys (no silent typos).
- Resolution into standard `SourceConfig` and `PipelineConfig` internal contracts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from accelerator.bronze_ingestion import SourceConfig
from accelerator.contracts import (
    ColumnMapping,
    ColumnSchema,
    ExtraColumnsPolicy,
    FieldType,
    MissingColumnsPolicy,
    PipelineConfig,
    QualityRule,
    Scd2Config,
    SilverConfig,
    ValidationSeverity,
)

SUPPORTED_CONFIG_VERSIONS = {1}
SUPPORTED_FORMATS = {"csv", "json"}

ALLOWED_ROOT_KEYS = {"version", "defaults", "sources", "pipelines"}
ALLOWED_DEFAULTS_KEYS = {"paths", "metadata_store", "delimiter", "encoding"}
ALLOWED_DEFAULT_PATHS_KEYS = {"raw", "bronze", "silver", "quarantine"}
ALLOWED_SOURCE_KEYS = {"input_path", "format", "bronze_path", "metadata_store", "delimiter", "encoding"}
ALLOWED_PIPELINE_KEYS = {"source", "silver"}
ALLOWED_SILVER_KEYS = {
    "target_name",
    "silver_path",
    "rejected_path",
    "primary_keys",
    "extra_columns",
    "missing_columns",
    "columns",
    "mappings",
    "quality_rules",
    "scd2",
}
ALLOWED_COLUMN_KEYS = {"name", "data_type", "nullable", "description"}
ALLOWED_MAPPING_KEYS = {"source_column", "target_column", "transform", "default_value"}
ALLOWED_QUALITY_RULE_KEYS = {"rule_name", "columns", "severity", "params"}
ALLOWED_SCD2_KEYS = {
    "enabled",
    "business_keys",
    "tracking_columns",
    "effective_from_col",
    "effective_to_col",
    "is_current_col",
}


def _check_allowed_keys(data: dict[str, Any], allowed: set[str], context: str) -> None:
    """Ensure data dictionary contains only allowed keys, rejecting typos and unknown attributes."""
    if not isinstance(data, dict):
        raise ValueError(f"{context} must be a dictionary/mapping, got {type(data).__name__}")
    unknown = set(data.keys()) - allowed
    if unknown:
        unknown_sorted = ", ".join(sorted(unknown))
        allowed_sorted = ", ".join(sorted(allowed))
        raise ValueError(
            f"Unknown or unrecognized key(s) in {context}: {unknown_sorted}. "
            f"Allowed keys are: {allowed_sorted}"
        )


@dataclass(frozen=True)
class PathDefaults:
    raw: Path
    bronze: Path
    silver: Path
    quarantine: Path


@dataclass(frozen=True)
class GlobalDefaults:
    paths: PathDefaults
    metadata_store: Path
    delimiter: str = ","
    encoding: str = "utf-8"


@dataclass(frozen=True)
class CentralConfig:
    version: int
    config_dir: Path
    defaults: GlobalDefaults
    sources: dict[str, SourceConfig] = field(default_factory=dict)
    pipelines: dict[str, PipelineConfig] = field(default_factory=dict)

    def get_source(self, name: str) -> SourceConfig:
        """Retrieve a resolved SourceConfig by name."""
        if name not in self.sources:
            available = ", ".join(sorted(self.sources.keys())) or "none"
            raise KeyError(f"Source '{name}' not found in configuration. Available sources: {available}")
        return self.sources[name]

    def get_pipeline(self, name: str) -> PipelineConfig:
        """Retrieve a resolved PipelineConfig by name."""
        if name not in self.pipelines:
            available = ", ".join(sorted(self.pipelines.keys())) or "none"
            raise KeyError(f"Pipeline '{name}' not found in configuration. Available pipelines: {available}")
        return self.pipelines[name]


def load_central_config(config_path: str | Path) -> CentralConfig:
    """Load, parse, validate, and resolve a central `config.yml` file.

    Parameters
    ----------
    config_path:
        Path to the YAML configuration file.

    Returns
    -------
    CentralConfig:
        Immutable container with resolved default paths, sources, and pipelines.

    Raises
    ------
    FileNotFoundError:
        If config_path does not exist.
    ValueError:
        If YAML is malformed, version unsupported, required keys missing, or unknown keys present.
    """
    path = Path(config_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    content = path.read_text(encoding="utf-8")
    try:
        raw = yaml.safe_load(content)
    except yaml.YAMLError as err:
        raise ValueError(f"Malformed YAML in '{path}': {err}") from err

    if not isinstance(raw, dict):
        raise ValueError(f"Root of configuration '{path}' must be a mapping/dictionary")

    _check_allowed_keys(raw, ALLOWED_ROOT_KEYS, "root configuration")

    # 1. Version validation
    if "version" not in raw:
        raise ValueError(f"Missing required 'version' key in '{path}'")
    version = raw["version"]
    if not isinstance(version, int) or version not in SUPPORTED_CONFIG_VERSIONS:
        supported_str = ", ".join(str(v) for v in sorted(SUPPORTED_CONFIG_VERSIONS))
        raise ValueError(f"Unsupported configuration version: {version}. Supported: {supported_str}")

    config_dir = path.parent

    def resolve_path(val: str | Path | None, default_dir: Path | None = None, filename: str | None = None) -> Path:
        """Resolve a path relative to config_dir."""
        if val is None:
            if default_dir is not None and filename is not None:
                return (default_dir / filename).resolve()
            raise ValueError("Cannot resolve empty path without default directory and filename")
        p = Path(val)
        return p if p.is_absolute() else (config_dir / p).resolve()

    # 2. Defaults section validation
    defaults_raw = raw.get("defaults", {})
    if not isinstance(defaults_raw, dict):
        raise ValueError("'defaults' must be a mapping/dictionary")
    _check_allowed_keys(defaults_raw, ALLOWED_DEFAULTS_KEYS, "defaults section")

    paths_raw = defaults_raw.get("paths", {})
    if not isinstance(paths_raw, dict):
        raise ValueError("'defaults.paths' must be a mapping/dictionary")
    _check_allowed_keys(paths_raw, ALLOWED_DEFAULT_PATHS_KEYS, "defaults.paths section")

    default_raw_dir = resolve_path(paths_raw.get("raw", "data/raw"))
    default_bronze_dir = resolve_path(paths_raw.get("bronze", "data/bronze"))
    default_silver_dir = resolve_path(paths_raw.get("silver", "data/silver"))
    default_quarantine_dir = resolve_path(paths_raw.get("quarantine", "data/quarantine"))

    path_defaults = PathDefaults(
        raw=default_raw_dir,
        bronze=default_bronze_dir,
        silver=default_silver_dir,
        quarantine=default_quarantine_dir,
    )

    default_metadata_store = resolve_path(defaults_raw.get("metadata_store", "data/metadata/ingestion.db"))
    default_delimiter = defaults_raw.get("delimiter", ",")
    if not isinstance(default_delimiter, str) or len(default_delimiter) != 1:
        raise ValueError("defaults.delimiter must be a single character string")

    default_encoding = defaults_raw.get("encoding", "utf-8")
    if not isinstance(default_encoding, str) or not default_encoding.strip():
        raise ValueError("defaults.encoding must be a non-empty string")

    global_defaults = GlobalDefaults(
        paths=path_defaults,
        metadata_store=default_metadata_store,
        delimiter=default_delimiter,
        encoding=default_encoding,
    )

    # 3. Sources validation & resolution
    sources_raw = raw.get("sources", {})
    if not isinstance(sources_raw, dict):
        raise ValueError("'sources' must be a mapping/dictionary of source definitions")

    resolved_sources: dict[str, SourceConfig] = {}
    for src_name, src_def in sources_raw.items():
        if not isinstance(src_name, str) or not src_name.strip():
            raise ValueError("Source names must be non-empty strings")
        _check_allowed_keys(src_def, ALLOWED_SOURCE_KEYS, f"sources.{src_name}")

        if "input_path" not in src_def:
            raise ValueError(f"Source '{src_name}' is missing required 'input_path'")

        src_format = src_def.get("format", "csv").lower()
        if src_format not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Source '{src_name}' has unsupported format '{src_format}'. Supported: {sorted(SUPPORTED_FORMATS)}"
            )

        src_input_path = resolve_path(src_def["input_path"])
        # bronze_path: custom or derived from default bronze directory
        if "bronze_path" in src_def and src_def["bronze_path"]:
            src_bronze_path = resolve_path(src_def["bronze_path"])
        else:
            src_bronze_path = (default_bronze_dir / f"{src_name}_raw.jsonl").resolve()

        if "metadata_store" in src_def and src_def["metadata_store"]:
            src_metadata_store = resolve_path(src_def["metadata_store"])
        else:
            src_metadata_store = default_metadata_store

        src_delimiter = src_def.get("delimiter", default_delimiter)
        if not isinstance(src_delimiter, str) or len(src_delimiter) != 1:
            raise ValueError(f"Source '{src_name}' delimiter must be a single character string")

        src_encoding = src_def.get("encoding", default_encoding)
        if not isinstance(src_encoding, str) or not src_encoding.strip():
            raise ValueError(f"Source '{src_name}' encoding must be a non-empty string")

        resolved_sources[src_name] = SourceConfig(
            source_name=src_name,
            format=src_format,
            input_path=src_input_path,
            bronze_path=src_bronze_path,
            metadata_store=src_metadata_store,
            delimiter=src_delimiter,
            encoding=src_encoding,
        )

    # 4. Pipelines validation & resolution
    pipelines_raw = raw.get("pipelines", {})
    if not isinstance(pipelines_raw, dict):
        raise ValueError("'pipelines' must be a mapping/dictionary of pipeline definitions")

    resolved_pipelines: dict[str, PipelineConfig] = {}
    for pipe_name, pipe_def in pipelines_raw.items():
        if not isinstance(pipe_name, str) or not pipe_name.strip():
            raise ValueError("Pipeline names must be non-empty strings")
        _check_allowed_keys(pipe_def, ALLOWED_PIPELINE_KEYS, f"pipelines.{pipe_name}")

        src_ref = pipe_def.get("source", pipe_name)
        if src_ref not in resolved_sources:
            raise ValueError(
                f"Pipeline '{pipe_name}' references source '{src_ref}' which is not declared under 'sources'"
            )
        base_source = resolved_sources[src_ref]

        silver_def = pipe_def.get("silver")
        silver_config: SilverConfig | None = None
        if silver_def is not None:
            if not isinstance(silver_def, dict):
                raise ValueError(f"Pipeline '{pipe_name}' silver configuration must be a mapping/dictionary")
            _check_allowed_keys(silver_def, ALLOWED_SILVER_KEYS, f"pipelines.{pipe_name}.silver")

            target_name = silver_def.get("target_name", f"dim_{pipe_name}")
            if "silver_path" in silver_def and silver_def["silver_path"]:
                silver_path = resolve_path(silver_def["silver_path"])
            else:
                silver_path = (default_silver_dir / f"{target_name}.jsonl").resolve()

            if "rejected_path" in silver_def and silver_def["rejected_path"]:
                rejected_path = resolve_path(silver_def["rejected_path"])
            else:
                rejected_path = (default_quarantine_dir / f"quarantine_{pipe_name}.jsonl").resolve()

            primary_keys = [str(k).strip() for k in silver_def.get("primary_keys", [])]

            extra_cols_str = str(silver_def.get("extra_columns", "ignore")).lower()
            try:
                extra_columns = ExtraColumnsPolicy(extra_cols_str)
            except ValueError as err:
                valid_policies = [p.value for p in ExtraColumnsPolicy]
                raise ValueError(
                    f"Pipeline '{pipe_name}' unknown extra_columns '{extra_cols_str}'. Valid: {', '.join(valid_policies)}"
                ) from err

            miss_cols_val = silver_def.get("missing_columns")
            miss_cols_str = "null" if miss_cols_val is None or str(miss_cols_val).lower() == "none" else str(miss_cols_val).lower()
            try:
                missing_columns = MissingColumnsPolicy(miss_cols_str)
            except ValueError as err:
                valid_policies = [p.value for p in MissingColumnsPolicy]
                raise ValueError(
                    f"Pipeline '{pipe_name}' unknown missing_columns '{miss_cols_str}'. Valid: {', '.join(valid_policies)}"
                ) from err

            # Columns
            columns: list[ColumnSchema] = []
            for col_dict in silver_def.get("columns", []):
                _check_allowed_keys(col_dict, ALLOWED_COLUMN_KEYS, f"pipelines.{pipe_name}.silver.columns")
                columns.append(ColumnSchema.from_dict(col_dict))

            # Mappings
            mappings: list[ColumnMapping] = []
            for map_dict in silver_def.get("mappings", []):
                _check_allowed_keys(map_dict, ALLOWED_MAPPING_KEYS, f"pipelines.{pipe_name}.silver.mappings")
                mappings.append(ColumnMapping.from_dict(map_dict))

            # Quality rules
            quality_rules: list[QualityRule] = []
            for q_dict in silver_def.get("quality_rules", []):
                _check_allowed_keys(q_dict, ALLOWED_QUALITY_RULE_KEYS, f"pipelines.{pipe_name}.silver.quality_rules")
                quality_rules.append(QualityRule.from_dict(q_dict))

            # SCD2
            scd2_dict = silver_def.get("scd2")
            if scd2_dict is not None:
                if not isinstance(scd2_dict, dict):
                    raise ValueError(f"Pipeline '{pipe_name}' scd2 must be a mapping/dictionary")
                _check_allowed_keys(scd2_dict, ALLOWED_SCD2_KEYS, f"pipelines.{pipe_name}.silver.scd2")
                scd2 = Scd2Config.from_dict(scd2_dict)
            else:
                scd2 = Scd2Config(enabled=False)

            # Semantic cross-validation
            defined_col_names = {col.name for col in columns}
            if defined_col_names:
                for m in mappings:
                    if m.target_column not in defined_col_names:
                        raise ValueError(
                            f"Pipeline '{pipe_name}' mapping target '{m.target_column}' is not defined in silver columns"
                        )
                for pk in primary_keys:
                    if pk not in defined_col_names:
                        raise ValueError(
                            f"Pipeline '{pipe_name}' primary_key '{pk}' is not defined in silver columns"
                        )
                if scd2.enabled:
                    for bk in scd2.business_keys:
                        if bk not in defined_col_names:
                            raise ValueError(
                                f"Pipeline '{pipe_name}' SCD2 business_key '{bk}' is not defined in silver columns"
                            )
                    for tc in scd2.tracking_columns:
                        if tc not in defined_col_names:
                            raise ValueError(
                                f"Pipeline '{pipe_name}' SCD2 tracking_column '{tc}' is not defined in silver columns"
                            )

            silver_config = SilverConfig(
                target_name=target_name,
                silver_path=silver_path,
                rejected_path=rejected_path,
                primary_keys=primary_keys,
                extra_columns=extra_columns,
                missing_columns=missing_columns,
                columns=columns,
                mappings=mappings,
                quality_rules=quality_rules,
                scd2=scd2,
            )

        resolved_pipelines[pipe_name] = PipelineConfig(
            source_name=base_source.source_name,
            format=base_source.format,
            input_path=base_source.input_path,
            bronze_path=base_source.bronze_path,
            metadata_store=base_source.metadata_store,
            delimiter=base_source.delimiter,
            encoding=base_source.encoding,
            silver=silver_config,
        )

    return CentralConfig(
        version=version,
        config_dir=config_dir,
        defaults=global_defaults,
        sources=resolved_sources,
        pipelines=resolved_pipelines,
    )
