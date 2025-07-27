from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
    dag_id="transform_datamart",
    schedule="30 2 * * *",
    default_args=default_args,
    catchup=False,
    tags=['dbt', 'transform', 'daily', 'bigquery', 'datamart'],
    description="DAG to Transform DBT Bigquery Data Mart"
) as dag:

    start = EmptyOperator(task_id="start_load")
    end = EmptyOperator(task_id="end_load")
    # wait_for_transform_dimfact_dag_success = ExternalTaskSensor(
    #     task_id='wait_for_transform_dimfact_dag_success',
    #     external_dag_id='transform_dimfact',
    #     external_task_id='transform_fact_review',
    #     allowed_states=['success'],
    #     failed_states=['failed', 'skipped'],
    #     poke_interval=300,
    #     timeout=3600,
    #     mode='poke',
    #     execution_delta=timedelta(minutes=30)
    # )

    marts_customer_product_summary = BashOperator(
        task_id="marts_customer_product_summary",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models data_mart.marts_customer_product_summary
        """
    )

    marts_customer_transaction_summary = BashOperator(
        task_id="marts_customer_transaction_summary",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models data_mart.marts_customer_transaction_summary
        """
    )

    marts_payment_transaction_summary = BashOperator(
        task_id="marts_payment_transaction_summary",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models data_mart.marts_payment_transaction_summary
        """
    )

    marts_product_transaction_summary = BashOperator(
        task_id="marts_product_transaction_summary",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models data_mart.marts_product_transaction_summary
        """
    )

    marts_review_product_summary = BashOperator(
        task_id="marts_review_product_summary",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models data_mart.marts_review_product_summary
        """
    )

    marts_session_transaction_summary = BashOperator(
        task_id="marts_session_transaction_summary",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models data_mart.marts_session_transaction_summary
        """
    )

    start >>[
                marts_customer_product_summary,
                marts_customer_transaction_summary,
                marts_payment_transaction_summary,
                marts_product_transaction_summary,
                marts_review_product_summary,
                marts_session_transaction_summary
            ] >> end