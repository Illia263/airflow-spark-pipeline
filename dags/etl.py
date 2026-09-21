from datetime import datetime, timedelta
from decimal import Decimal
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import subprocess
import os
from airflow.sensors.filesystem import FileSensor
logger = logging.getLogger(__name__)

@dag(
    dag_id="ecommerce_data",
    schedule="@daily",
    start_date=pendulum.datetime(2019, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2019, 11, 30, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["ecommerce", "2019"],
)
def ecommerce_pipeline():
    check_db_alive = PostgresOperator(
        task_id = "check_db_alive",
        postgres_conn_id="postgres_conn_id",
        sql="SELECT 1;"
    )
    wait_for_reply = FileSensor(
        task_id="wait_for_reply",
        filepath="/opt/airflow/data/lake/raw/events/{{ execution_date.strftime('%Y-%b') }}/event_date={{ ds }}/",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="poke"
    )

    clear_old_data = PostgresOperator(
        task_id="clear_old_data",
        postgres_conn_id="postgres_conn_id",
        sql="DELETE FROM daily_sales WHERE event_date = '{{ ds }}';"
    )
    @task(
        retries=3,
        retry_delay=timedelta(seconds=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=60)
    )
    def extract(ds=None, data_interval_start=None):
        hook = PostgresHook(postgres_conn_id="postgres_conn_id")
        conn_info = hook.get_connection(hook.postgres_conn_id)
        env = os.environ.copy()
        env['DB_HOST'] = conn_info.host
        env['DB_PORT'] = str(conn_info.port)
        env['DB_NAME'] = conn_info.schema
        env['DB_USER'] = conn_info.login
        env['DB_PASSWORD'] = conn_info.password
        script_run = subprocess.run([
            "spark-submit", 
            "--packages", "org.postgresql:postgresql:42.6.0", 
            "/opt/airflow/dags/daily_etl.py", ds
            ], env=env, check=True, capture_output=True, text=True)
    extract_task = extract()
    check_db_alive >> wait_for_reply >> clear_old_data >> extract_task
ecommerce_pipeline()
#docker exec airflow_ecommerce cat standalone_admin_password.txt 