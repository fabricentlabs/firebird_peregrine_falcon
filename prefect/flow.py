from prefect.variables import Variable
from prefect import flow, task
from pathlib import Path
import subprocess


@task
def run_sh_from_github(database: str, outDir: str, table: str) -> str:
    """Executa script bash do repositório com parâmetros"""
    
    # Descobrir onde o script está
    script_dir = Path(__file__).parent  # prefect/
    repo_root = script_dir.parent       # firebird_peregrine_falcon/
    sh_file = repo_root / "run_agile_log_obrigacao.sh"
    
    if not sh_file.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {sh_file}")
    
    print(f"Executando: {sh_file}")
    print(f"Database: {database}, OutDir: {outDir}, Table: {table}")
    
    # Executar script bash com parâmetros
    result = subprocess.run(
        [
            "bash",
            str(sh_file),
            database,
            outDir,
            table
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root)
    )
    
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"Script bash falhou com código {result.returncode}")
    
    return result.stdout


@flow(name="Get data from firebird", log_prints=True)
def load_data_from_firebird() -> dict:
    
    """Flow principal que executa o script para cada tabela"""
    
    database = Variable.get("firebird")
    outDir = Variable.get("outdir")
    tables_str = Variable.get("tables")
    tables = [table for table in tables_str.split(",")]
      
    results = {}   
    
    # Processar todas as tabelas
    for table in tables:
        print(f"\n=== Processando: {table} ===")
        try:
            output = run_sh_from_github(database, outDir, table)
            results[table] = {"status": "success", "output": output}
            print(f"✅ {table} extraído com sucesso!")
        except Exception as e:
            results[table] = {"status": "error", "error": str(e)}
            print(f"❌ Erro ao processar {table}: {e}")
    
    # Resumo final
    print(f"\n=== Resumo Final ===")
    for table, result in results.items():
        status = result["status"]
        print(f"{table}: {status}")
    
    return results


if __name__ == "__main__":    
    load_data_from_firebird()
