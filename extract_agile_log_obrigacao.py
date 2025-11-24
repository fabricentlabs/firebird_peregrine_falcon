#!/usr/bin/env python3
"""
Prefect flow for extracting AGILE_LOG_OBRIGACAO from Firebird database.
Works on both Windows and Ubuntu/Linux systems.
"""

import os
import sys
import subprocess
import platform
from pathlib import Path
from typing import Optional
from prefect import flow, task, get_run_logger
from prefect.tasks import task_inputs
from prefect.blocks.system import Secret


@task(name="build-rust-binary", log_prints=True)
def build_rust_binary(project_root: Path) -> bool:
    """Build the Rust binary if it doesn't exist."""
    logger = get_run_logger()
    
    system = platform.system()
    if system == "Windows":
        binary_name = "firebird_peregrine_falcon.exe"
    else:
        binary_name = "firebird_peregrine_falcon"
    
    binary_path = project_root / "target" / "release" / binary_name
    
    if binary_path.exists():
        logger.info(f"Binary already exists: {binary_path}")
        return True
    
    logger.info("Building Rust binary...")
    try:
        result = subprocess.run(
            ["cargo", "build", "--release"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("Build completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Build failed: {e.stderr}")
        return False
    except FileNotFoundError:
        logger.error("Cargo not found. Please install Rust toolchain.")
        return False


@task(name="run-extraction", log_prints=True)
def run_extraction(
    database: str,
    out_dir: str,
    table: str,
    parallelism: int = 40,
    pool_size: int = 80,
    user: str = "SYSDBA",
    password: str = "masterkey",
    use_compression: bool = False,
    project_root: Optional[Path] = None
) -> bool:
    """Run the Firebird extraction using the Rust binary."""
    logger = get_run_logger()
    
    if project_root is None:
        project_root = Path(__file__).parent
    
    system = platform.system()
    if system == "Windows":
        binary_name = "firebird_peregrine_falcon.exe"
    else:
        binary_name = "firebird_peregrine_falcon"
    
    binary_path = project_root / "target" / "release" / binary_name
    
    if not binary_path.exists():
        logger.error(f"Binary not found: {binary_path}")
        logger.info("Attempting to build...")
        if not build_rust_binary(project_root):
            return False
    
    logger.info("=== FIREBIRD PEREGRINE FALCON ===")
    logger.info(f"Extracting: {table}")
    logger.info(f"Database: {database}")
    logger.info(f"Output: {out_dir}")
    logger.info(f"Parallelism: {parallelism}")
    logger.info(f"Pool size: {pool_size}")
    
    # Prepare command
    cmd = [
        str(binary_path),
        "--database", database,
        "--out-dir", out_dir,
        "--table", table,
        "--parallelism", str(parallelism),
        "--pool-size", str(pool_size),
        "--user", user,
        "--password", password,
    ]
    
    if use_compression:
        cmd.append("--use-compression")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        logger.info("✅ Extraction completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Extraction failed: {e.stderr}")
        logger.error(f"Return code: {e.returncode}")
        return False


@flow(name="extract-agile-log-obrigacao", log_prints=True)
def extract_agile_log_obrigacao_flow(
    database: Optional[str] = None,
    out_dir: Optional[str] = None,
    table: str = "AGILE_LOG_OBRIGACAO",
    parallelism: int = 40,
    pool_size: int = 80,
    user: str = "SYSDBA",
    password: Optional[str] = None,
    use_compression: bool = False,
    project_root: Optional[str] = None,
    auto_build: bool = True
):
    """
    Prefect flow to extract AGILE_LOG_OBRIGACAO from Firebird database.
    
    Args:
        database: Path to Firebird database file. If None, uses environment variable or default.
        out_dir: Output directory for Parquet files. If None, uses environment variable or default.
        table: Table name to extract (default: AGILE_LOG_OBRIGACAO)
        parallelism: Number of parallel workers (default: 40)
        pool_size: Connection pool size (default: 80)
        user: Firebird username (default: SYSDBA)
        password: Firebird password. If None, uses environment variable or default.
        use_compression: Enable compression (default: False)
        project_root: Root directory of the Rust project. If None, uses current directory.
        auto_build: Automatically build binary if not found (default: True)
    """
    logger = get_run_logger()
    
    # Determine project root
    if project_root:
        project_root = Path(project_root)
    else:
        project_root = Path(__file__).parent
    
    # Get configuration from environment variables or use defaults
    system = platform.system()
    
    if database is None:
        database = os.getenv(
            "FIREBIRD_DATABASE",
            "/path/to/database.fdb" if system != "Windows" else r"D:\Clients\Legnet\SISLEG_RESTORED.FDB"
        )
    
    if out_dir is None:
        out_dir = os.getenv(
            "FIREBIRD_OUT_DIR",
            "/data/output" if system != "Windows" else r"D:\Clients\Legnet\powerleg\data-sat"
        )
    
    if password is None:
        password = os.getenv("FIREBIRD_PASSWORD", "masterkey")
    
    logger.info(f"Platform: {system}")
    logger.info(f"Project root: {project_root}")
    
    # Build binary if needed
    if auto_build:
        build_rust_binary(project_root)
    
    # Run extraction
    success = run_extraction(
        database=database,
        out_dir=out_dir,
        table=table,
        parallelism=parallelism,
        pool_size=pool_size,
        user=user,
        password=password,
        use_compression=use_compression,
        project_root=project_root
    )
    
    if not success:
        raise Exception("Extraction failed")
    
    return success


if __name__ == "__main__":
    # Allow running as a script with command-line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract AGILE_LOG_OBRIGACAO using Prefect")
    parser.add_argument("--database", type=str, help="Firebird database path")
    parser.add_argument("--out-dir", type=str, dest="out_dir", help="Output directory")
    parser.add_argument("--table", type=str, default="AGILE_LOG_OBRIGACAO", help="Table name")
    parser.add_argument("--parallelism", type=int, default=40, help="Number of parallel workers")
    parser.add_argument("--pool-size", type=int, default=80, dest="pool_size", help="Connection pool size")
    parser.add_argument("--user", type=str, default="SYSDBA", help="Firebird username")
    parser.add_argument("--password", type=str, help="Firebird password")
    parser.add_argument("--use-compression", action="store_true", dest="use_compression", help="Enable compression")
    parser.add_argument("--project-root", type=str, dest="project_root", help="Rust project root directory")
    parser.add_argument("--no-auto-build", action="store_true", dest="no_auto_build", help="Don't auto-build binary")
    
    args = parser.parse_args()
    
    extract_agile_log_obrigacao_flow(
        database=args.database,
        out_dir=args.out_dir,
        table=args.table,
        parallelism=args.parallelism,
        pool_size=args.pool_size,
        user=args.user,
        password=args.password,
        use_compression=args.use_compression,
        project_root=args.project_root,
        auto_build=not args.no_auto_build
    )

