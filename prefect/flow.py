from prefect import flow, task
import subprocess
from pathlib import Path



@task
def run_ps1_from_github(database: str, outDir: str, table: str) -> None:
    # Descobrir onde o script está
    script_dir = Path(__file__).parent  # prefect/
    repo_root = script_dir.parent       # firebird_peregrine_falcon/
    ps1_file = repo_root / "run_agile_log_obrigacao.ps1"
    
    if not ps1_file.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {ps1_file}")
    
    print(f"Executando: {ps1_file}")
    print(f"Database: {database}, OutDir: {outDir}, Table: {table}")
    
    result = subprocess.run(
        [
            "powershell.exe",
            "-ExecutionPolicy", "Bypass",
            "-File", str(ps1_file),
            "-database", database,      # ← lowercase
            "-outDir", outDir,          # ← lowercase
            "-table", table             # ← lowercase
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root)
    )
    
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"PowerShell falhou com código {result.returncode}")
    
    return result.stdout

@flow(name="Get data from firebird", log_prints=True)
def load_data_from_firebird(database: str, outDir: str, tables: list[str]) -> None:
    
    results = {}
    
    # ✅ IMPORTANTE: NÃO ter return aqui!
    for table in tables:
        print(f"\n=== Processando: {table} ===")
        try:
            output = run_ps1_from_github(database, outDir, table)
            results[table] = {"status": "success", "output": output}
            print(f"✅ {table} extraído com sucesso!")
        except Exception as e:
            results[table] = {"status": "error", "error": str(e)}
            print(f"❌ Erro ao processar {table}: {e}")
    
    # Return DEPOIS do loop, não dentro
    print(f"\n=== Resumo Final ===")
    for table, result in results.items():
        status = result["status"]
        print(f"{table}: {status}")
    
    return results


if __name__ == "__main__":
    load_data_from_firebird()
