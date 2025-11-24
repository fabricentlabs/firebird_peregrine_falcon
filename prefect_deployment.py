"""
Prefect deployment configuration for extract_agile_log_obrigacao flow.
Run this to create a deployment that can be scheduled or triggered via API.

Usage:
    python prefect_deployment.py
    
Or use Prefect CLI:
    prefect deploy prefect_deployment.py:extract_agile_log_obrigacao_flow
"""

from prefect import serve
from prefect.schedules import CronSchedule
from extract_agile_log_obrigacao import extract_agile_log_obrigacao_flow

if __name__ == "__main__":
    # Create a deployment that can be scheduled or triggered
    # Using serve() for long-running deployments with UI
    extract_agile_log_obrigacao_flow.serve(
        name="agile-log-obrigacao-extraction",
        description="Extract AGILE_LOG_OBRIGACAO table from Firebird database",
        tags=["firebird", "extraction", "agile-log-obrigacao"],
        # Uncomment to add a schedule:
        # schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),  # Daily at 2 AM UTC
    )

