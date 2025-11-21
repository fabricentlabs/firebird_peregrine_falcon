from prefect_github import GitHubRepository
from prefect import flow, task
import subprocess
import os

@task
def run_ps1_from_github(dataabse: str, outDir: str, table: str) -> None:
    # Carrega o bloco GitHub criado no Prefect UI/CLI
    repo = GitHubRepository.load("firebird_peregrine_falcon")

    # Clona / baixa o repositório para um diretório temporário
    repo_dir = repo.get_directory()

    # Caminho do script dentro do repositório clonado
    ps1_path = os.path.join(
        repo_dir,
        "run_agile_log_obrigacao.ps1"
    )

    # Executa o script
    result = subprocess.run(
        [
            "powershell.exe",
            "-ExecutionPolicy", "Bypass",
            "-File", ps1_path,
        ],
        capture_output=True,
        text=True
    )

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    return result.stdout


@flow(name="Get data from firebird", log_prints=True)
def load_data_from_firebird(database: str, outDir: str, tables: list[str]) -> None:
    
    for table in tables:
        return run_ps1_from_github(database, outDir, table)


if __name__ == "__main__":
    load_data_from_firebird(
        database='C:\\consumer\\CONSUMER.FDB',
        outDir='C:\\PARA\\1Projects\\Fabricent_Legnet\\data',
        tables=['ITENSPEDIDO', 'PEDIDOS', 'PRODUTODETALHE']
    )
