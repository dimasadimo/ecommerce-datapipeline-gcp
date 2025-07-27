from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from pendulum import timezone, datetime
import sys
sys.path.insert(0, "/opt/airflow")
from utils.failed_notification import failed_notification

local_tz = timezone("Asia/Jakarta")

default_args = {
    'owner': 'Dimas Adi Hartomo',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 7, 1, 0, 0, 0, tz=local_tz),
    'on_failure_callback': failed_notification
}

with DAG(
    dag_id="load_postgresql",
    schedule_interval='@hourly',
    default_args=default_args,
    catchup=False,
    tags=['load', 'postgresql', 'hourly'],
    description="DAG to Load Data PostgreSql"
) as dag:

    load_postgresql = BashOperator(
        task_id="load_postgresql",
        bash_command="""
        set -e;
        cd /opt/airflow/script && \
        python main.py
        """
    )

    load_postgresql