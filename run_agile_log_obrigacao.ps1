# Run firebird_peregrine_falcon to extract agile_log_obrigacao
# This can run in parallel with stone_as_fast (extracting obrigacao_gcl_setor)

param(
    [string]$database,
    [string]$outDir,
    [string]$table
)

Write-Host "=== FIREBIRD PEREGRINE FALCON ===" -ForegroundColor Cyan

# DEBUG: Verificar valores recebidos
Write-Host "DEBUG - Valores recebidos:" -ForegroundColor Yellow
Write-Host "  database: '$database'" -ForegroundColor Gray
Write-Host "  outDir: '$outDir'" -ForegroundColor Gray
Write-Host "  table: '$table'" -ForegroundColor Gray
Write-Host ""

# Validar se parâmetros não estão vazios
if ([string]::IsNullOrWhiteSpace($database)) {
    Write-Host "Erro: database não pode estar vazio!" -ForegroundColor Red
    exit 1
}
if ([string]::IsNullOrWhiteSpace($outDir)) {
    Write-Host "Erro: outDir não pode estar vazio!" -ForegroundColor Red
    exit 1
}
if ([string]::IsNullOrWhiteSpace($table)) {
    Write-Host "Erro: table não pode estar vazio!" -ForegroundColor Red
    exit 1
}

Write-Host "Extracting: $table" -ForegroundColor White
Write-Host "Database: $database" -ForegroundColor Gray
Write-Host "Output: $outDir" -ForegroundColor Gray
Write-Host ""

# Verificar se executável existe
$exePath = ".\target\release\firebird_peregrine_falcon.exe"
if (-not (Test-Path $exePath)) {
    Write-Host "Erro: Executável não encontrado em $exePath" -ForegroundColor Red
    exit 1
}

# Run extraction com QUOTED parameters
Write-Host "Executando comando:" -ForegroundColor Yellow
Write-Host "& '$exePath' --database '$database' --out-dir '$outDir' --table '$table' --parallelism 40 --pool-size 80" -ForegroundColor Gray

& "$exePath" `
    --database "$database" `
    --out-dir "$outDir" `
    --table "$table" `
    --parallelism 40 `
    --pool-size 80

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Extraction completed successfully!" -ForegroundColor Green
} else {
    Write-Host "`n❌ Extraction failed!" -ForegroundColor Red
    Write-Host "Exit code: $LASTEXITCODE" -ForegroundColor Red
    exit 1
}
