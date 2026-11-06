"""Metadata-driven Lakehouse framework: YAML-configured PySpark + Delta Lake loads."""

__version__ = "0.1.0"

from .config import ConfigError, FrameworkConfig, TableConfig, load_config, parse_config  # noqa: E402

__all__ = [
    "ConfigError",
    "FrameworkConfig",
    "TableConfig",
    "load_config",
    "parse_config",
    "__version__",
]
