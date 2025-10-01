# dags/btc_hourly_fetch_dag.py
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="btc_hourly_fetch",
    # #WHY: '@hourly' is a preset that means "run at the beginning of every hour".
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    catchup=False,
    tags=["btc", "etl", "hourly"],
    doc_md="Hourly DAG to fetch the latest k-line data from Binance API.",
) as dag:

    DockerOperator(
        task_id='run_backfill_script_hourly',
        image='cofirazak/btc-scripts:latest',
        command='python src/backfill_script.py',
        auto_remove='success',
        docker_url="unix://var/run/docker.sock",
        network_mode="host"
    )
