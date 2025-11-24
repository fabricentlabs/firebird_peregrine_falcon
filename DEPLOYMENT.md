# Deployment Guide

This project now supports multiple deployment options for extracting `AGILE_LOG_OBRIGACAO` from Firebird databases:

1. **PowerShell Script** (Windows) - Original script
2. **Bash Script** (Ubuntu/Linux) - Direct execution
3. **Prefect Flow** (Cross-platform) - Orchestrated execution with monitoring

## Prerequisites

### For All Options
- Rust toolchain installed (`cargo` command available)
- Firebird database accessible
- Appropriate permissions to read database and write output directory

### For Prefect Option
- Python 3.8+
- Prefect installed: `pip install -r requirements.txt`

## Option 1: PowerShell Script (Windows)

**File**: `run_agile_log_obrigacao.ps1`

```powershell
# Edit the script to set your paths, then run:
.\run_agile_log_obrigacao.ps1
```

**Configuration**: Edit the script to modify:
- `$database` - Path to Firebird database
- `$outDir` - Output directory for Parquet files

## Option 2: Bash Script (Ubuntu/Linux)

**File**: `run_agile_log_obrigacao.sh`

```bash
# Make executable
chmod +x run_agile_log_obrigacao.sh

# Set environment variables (optional)
export FIREBIRD_DATABASE="/path/to/database.fdb"
export FIREBIRD_OUT_DIR="/data/output"

# Run
./run_agile_log_obrigacao.sh
```

**Configuration**: 
- Edit the script to modify default paths, or
- Set environment variables: `FIREBIRD_DATABASE` and `FIREBIRD_OUT_DIR`

## Option 3: Prefect Flow (Recommended for Production)

### Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Start Prefect server** (optional, for UI):
   ```bash
   prefect server start
   ```
   Access UI at http://localhost:4200

### Running the Flow

#### Method A: Direct Execution

```bash
# Run with default configuration
python extract_agile_log_obrigacao.py

# Run with custom parameters
python extract_agile_log_obrigacao.py \
    --database "/path/to/database.fdb" \
    --out-dir "/data/output" \
    --parallelism 40 \
    --pool-size 80
```

#### Method B: Using Prefect CLI

```bash
# Run flow directly
prefect deployment run extract-agile-log-obrigacao

# Or with parameters via environment variables
export FIREBIRD_DATABASE="/path/to/database.fdb"
export FIREBIRD_OUT_DIR="/data/output"
prefect deployment run extract-agile-log-obrigacao
```

#### Method C: Create a Deployment (for Scheduling)

1. **Create deployment**:
   ```bash
   python prefect_deployment.py
   ```

2. **Schedule or trigger**:
   ```bash
   # View in Prefect UI
   # Or trigger via CLI:
   prefect deployment run agile-log-obrigacao-extraction
   ```

### Configuration

The Prefect flow supports multiple configuration methods:

1. **Command-line arguments** (highest priority)
2. **Environment variables**:
   - `FIREBIRD_DATABASE` - Database path
   - `FIREBIRD_OUT_DIR` - Output directory
   - `FIREBIRD_PASSWORD` - Database password

3. **Default values** (platform-specific):
   - Windows: Uses Windows-style paths
   - Linux/Ubuntu: Uses Unix-style paths

### Example: Running on Ubuntu with Prefect

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
export FIREBIRD_DATABASE="/mnt/firebird/SISLEG_RESTORED.FDB"
export FIREBIRD_OUT_DIR="/data/powerleg/data-sat"
export FIREBIRD_PASSWORD="your_password"

# 3. Run the flow
python extract_agile_log_obrigacao.py

# Or create a deployment for scheduling
python prefect_deployment.py
```

### Prefect Cloud/Server Integration

For production deployments with Prefect Cloud or a Prefect server:

1. **Set up Prefect workspace**:
   ```bash
   prefect cloud login
   # or
   prefect config set PREFECT_API_URL="http://your-prefect-server:4200/api"
   ```

2. **Create deployment**:
   ```bash
   python prefect_deployment.py
   ```

3. **Deploy to a work pool**:
   ```bash
   prefect deploy prefect_deployment.py:extract_agile_log_obrigacao_flow
   ```

## Environment-Specific Notes

### Windows
- Binary: `firebird_peregrine_falcon.exe`
- Default paths use Windows drive letters
- PowerShell script available

### Ubuntu/Linux
- Binary: `firebird_peregrine_falcon` (no .exe)
- Default paths use Unix-style paths
- Bash script available
- Prefect flow works identically

## Troubleshooting

### Binary Not Found
- Ensure Rust is installed: `cargo --version`
- Build manually: `cargo build --release`
- Check binary exists at: `target/release/firebird_peregrine_falcon[.exe]`

### Permission Issues
- Ensure read access to database file
- Ensure write access to output directory
- On Linux, may need `sudo` or proper user permissions

### Prefect Issues
- Ensure Prefect is installed: `pip install prefect`
- Check Prefect server is running (if using UI)
- Verify environment variables are set correctly

## Parallel Execution

This extraction can run in parallel with other extractions (e.g., `stone_as_fast`). The Prefect flow can be configured to run multiple extractions concurrently using Prefect's task dependencies and parallel execution features.

