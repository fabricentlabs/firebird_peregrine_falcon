#!/bin/bash

# Run firebird_peregrine_falcon to extract agile_log_obrigacao
# This can run in parallel with stone_as_fast (extracting obrigacao_gcl_setor)

# Validar argumentos
if [ $# -lt 3 ]; then
    echo "Erro: Faltam argumentos"
    echo "Uso: $0 <database> <outDir> <table>"
    exit 1
fi

database="$1"
outDir="$2"
table="$3"

echo "=== FIREBIRD PEREGRINE FALCON ==="
echo ""

# DEBUG: Verificar valores recebidos
echo "DEBUG - Valores recebidos:"
echo "  database: '$database'"
echo "  outDir: '$outDir'"
echo "  table: '$table'"
echo ""

# Validar se parâmetros não estão vazios
if [ -z "$database" ]; then
    echo "Erro: database não pode estar vazio!"
    exit 1
fi

if [ -z "$outDir" ]; then
    echo "Erro: outDir não pode estar vazio!"
    exit 1
fi

if [ -z "$table" ]; then
    echo "Erro: table não pode estar vazio!"
    exit 1
fi

echo "Extracting: $table"
echo "Database: $database"
echo "Output: $outDir"
echo ""

# Verificar se executável existe
exe_path="./target/release/firebird_peregrine_falcon"
if [ ! -f "$exe_path" ]; then
    echo "Erro: Executável não encontrado em $exe_path"
    exit 1
fi

# Run extraction com quoted parameters
echo "Executando comando:"
echo "$exe_path --database '$database' --out-dir '$outDir' --table '$table' --parallelism 40 --pool-size 80"
echo ""

# Executar
"$exe_path" \
    --database "$database" \
    --out-dir "$outDir" \
    --table "$table" \
    --parallelism 40 \
    --pool-size 80

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "✅ Extraction completed successfully!"
    exit 0
else
    echo ""
    echo "❌ Extraction failed!"
    echo "Exit code: $exit_code"
    exit 1
fi
