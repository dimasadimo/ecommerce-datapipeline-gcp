from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    dag_id="transform_dimfact",
    schedule="0 2 * * *",
    default_args=default_args,
    catchup=False,
    tags=['dbt', 'transform', 'daily', 'bigquery', 'dim', 'fact'],
    description="DAG to Transform DBT Bigquery"
) as dag:

    # wait_for_load_dag_success = ExternalTaskSensor(
    #     task_id='wait_for_load_dag_success',
    #     external_dag_id='load_gcs_to_bq',
    #     external_task_id='end_load',
    #     allowed_states=['success'],
    #     failed_states=['failed', 'skipped'],
    #     poke_interval=300,
    #     timeout=3600,
    #     mode='poke',
    #     execution_delta=timedelta(minutes=30)
    # )

    transform_dim_customer = BashOperator(
        task_id="transform_dim_customer",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models dim_table.dim_customer
        """
    )

    transform_dim_product = BashOperator(
        task_id="transform_dim_product",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models dim_table.dim_product
        """
    )

    transform_dim_payment = BashOperator(
        task_id="transform_dim_payment",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models dim_table.dim_payment
        """
    )

    transform_dim_session = BashOperator(
        task_id="transform_dim_session",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models dim_table.dim_session
        """
    )

    transform_fact_transaction = BashOperator(
        task_id="transform_fact_transaction",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models fact_table.fact_transaction
        """
    )

    transform_fact_review = BashOperator(
        task_id="transform_fact_review",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models fact_table.fact_review
        """
    )

    [
        transform_dim_customer,
        transform_dim_product,
        transform_dim_payment,
        transform_dim_session,
    ] >> transform_fact_transaction  >> transform_fact_review