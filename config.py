"""
Configuration file for Firebird extraction.
Modify these values or set environment variables for your environment.
"""

import os
from pathlib import Path
import platform

# Platform detection
SYSTEM = platform.system()
IS_WINDOWS = SYSTEM == "Windows"
IS_LINUX = SYSTEM == "Linux"

# Default paths - modify these for your environment
if IS_WINDOWS:
    DEFAULT_DATABASE = r"D:\Clients\Legnet\SISLEG_RESTORED.FDB"
    DEFAULT_OUT_DIR = r"D:\Clients\Legnet\powerleg\data-sat"
    BINARY_NAME = "firebird_peregrine_falcon.exe"
else:
    # Ubuntu/Linux defaults
    DEFAULT_DATABASE = "/path/to/database.fdb"  # Update this!
    DEFAULT_OUT_DIR = "/data/output"  # Update this!
    BINARY_NAME = "firebird_peregrine_falcon"

# Table configuration
DEFAULT_TABLE = "AGILE_LOG_OBRIGACAO"

# Performance settings
DEFAULT_PARALLELISM = 40
DEFAULT_POOL_SIZE = 80

# Firebird credentials
DEFAULT_USER = "SYSDBA"
DEFAULT_PASSWORD = os.getenv("FIREBIRD_PASSWORD", "masterkey")

# Other settings
USE_COMPRESSION = False

# Project root (where Cargo.toml is located)
PROJECT_ROOT = Path(__file__).parent

# Binary path
BINARY_PATH = PROJECT_ROOT / "target" / "release" / BINARY_NAME

