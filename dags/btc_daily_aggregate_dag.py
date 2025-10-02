# dags/btc_daily_aggregate_dag.py
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="btc_daily_aggregation",
    # #WHY: This cron expression means "at minute 5 past hour 8 every day".
    # 08:05 UTC is 11:05 Moscow Time (MSK), as required.
    schedule="5 8 * * *",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    catchup=False,
    tags=["btc", "etl", "daily"],
    doc_md="Daily DAG to aggregate hourly data into daily sessions and calculate statistics.",
) as dag:

    DockerOperator(
        task_id='run_aggregator_script_daily',
        image='cofirazak/btc-scripts:latest',
        command='python src/session_aggregator.py',
        auto_remove='success',
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        mount_tmp_dir=False
    )
