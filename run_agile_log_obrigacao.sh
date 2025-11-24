#!/bin/bash
# Run firebird_peregrine_falcon to extract agile_log_obrigacao on Ubuntu/Linux
# This can run in parallel with stone_as_fast (extracting obrigacao_gcl_setor)

# Configuration - modify these paths for your Ubuntu environment
DATABASE="${FIREBIRD_DATABASE:-/path/to/database.fdb}"
OUT_DIR="${FIREBIRD_OUT_DIR:-/data/output}"
TABLE="AGILE_LOG_OBRIGACAO"

echo "=== FIREBIRD PEREGRINE FALCON ==="
echo "Extracting: $TABLE"
echo "Database: $DATABASE"
echo "Output: $OUT_DIR"
echo ""

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_PATH="$SCRIPT_DIR/target/release/firebird_peregrine_falcon"

# Build if needed
if [ ! -f "$BINARY_PATH" ]; then
    echo "Building release version..."
    cargo build --release
    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit 1
    fi
fi

# Run extraction
"$BINARY_PATH" \
    --database "$DATABASE" \
    --out-dir "$OUT_DIR" \
    --table "$TABLE" \
    --parallelism 40 \
    --pool-size 80

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Extraction completed successfully!"
else
    echo ""
    echo "❌ Extraction failed!"
    exit 1
fi

