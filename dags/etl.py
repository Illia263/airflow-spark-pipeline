from datetime import datetime, timedelta
from decimal import Decimal
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
import logging
import subprocess
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
    @task(
        retries=3,
        retry_delay=timedelta(seconds=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=60)
    )
    def extract(ds=None, data_interval_start=None):
        script_run = subprocess.run([
        "spark-submit", 
        "--packages", "org.postgresql:postgresql:42.6.0", 
        "/opt/airflow/dags/daily_etl.py", 
        ds
    ], check=True)
    extract()
ecommerce_pipeline()