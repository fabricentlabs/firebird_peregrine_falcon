from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
from prefect.schedules import Schedule
from dotenv import load_dotenv
from prefect import flow
import os

if __name__ == "__main__":
    
    load_dotenv()
    git_url = os.getenv("GITHUB_URL")
    branch = os.getenv("GITHUB_BRANCH")
    entrypoint = os.getenv("ENTRYPOINT")
    deploy_name = os.getenv("DEPLOY_NAME")
    workpool = os.getenv("WORKPOOL")
    schedule = Schedule(cron="0 19 * * *", timezone='America/Sao_Paulo')
    
    
    source = GitRepository(
        url=git_url,
        branch=branch,
        credentials=GitHubCredentials.load("legnet-pernsonal-token")
    )

    flow.from_source(
        source=source,
        entrypoint=entrypoint
    ).deploy(
        name=deploy_name,
        work_pool_name=workpool,
        tags=["linux", "peregrine_falcon", "firebird"],
        description="Deploy git linux teste",
        schedule=schedule
    )
