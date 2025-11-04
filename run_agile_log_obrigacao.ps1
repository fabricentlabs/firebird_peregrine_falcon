# Run firebird_peregrine_falcon to extract agile_log_obrigacao
# This can run in parallel with stone_as_fast (extracting obrigacao_gcl_setor)

$database = "D:\Clients\Legnet\SISLEG_RESTORED.FDB"
$outDir = "D:\Clients\Legnet\powerleg\data-sat"
$table = "AGILE_LOG_OBRIGACAO"

Write-Host "=== FIREBIRD PEREGRINE FALCON ===" -ForegroundColor Cyan
Write-Host "Extracting: $table" -ForegroundColor White
Write-Host "Database: $database" -ForegroundColor Gray
Write-Host "Output: $outDir" -ForegroundColor Gray
Write-Host ""

# Build if needed
if (-not (Test-Path ".\target\release\firebird_peregrine_falcon.exe")) {
    Write-Host "Building release version..." -ForegroundColor Yellow
    cargo build --release
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed!" -ForegroundColor Red
        exit 1
    }
}

# Run extraction
.\target\release\firebird_peregrine_falcon.exe `
    --database $database `
    --out-dir $outDir `
    --table $table `
    --parallelism 40 `
    --pool-size 80

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Extraction completed successfully!" -ForegroundColor Green
} else {
    Write-Host "`n❌ Extraction failed!" -ForegroundColor Red
}

