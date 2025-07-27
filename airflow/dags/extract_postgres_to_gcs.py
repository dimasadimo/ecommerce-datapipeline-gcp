from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime, timedelta
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
    dag_id="extract_postgres_to_gcs",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=False,
    description="Extract Postgres to GCS in CSV"
) as dag:


    extract_customer_to_gcs = PostgresToGCSOperator(
        task_id="extract_customer_to_gcs",
        postgres_conn_id="postgres_con_gcs",
        sql="SELECT * FROM ecommerce.customer",
        bucket="jdeol003-bucket",
        filename="final_project_de03_dimasadihartomo/master/customer_stg.csv",
        export_format="csv",
        field_delimiter=",",
    )

    extract_product_to_gcs = PostgresToGCSOperator(
        task_id="extract_product_to_gcs",
        postgres_conn_id="postgres_con_gcs",
        sql="SELECT * FROM ecommerce.product",
        bucket="jdeol003-bucket",
        filename="final_project_de03_dimasadihartomo/master/product_stg.csv",
        export_format="csv",
        field_delimiter=",",
    )

    extract_payment_to_gcs = PostgresToGCSOperator(
        task_id="extract_payment_to_gcs",
        postgres_conn_id="postgres_con_gcs",
        sql="SELECT * FROM ecommerce.payment_method",
        bucket="jdeol003-bucket",
        filename="final_project_de03_dimasadihartomo/master/payment_stg.csv",
        export_format="csv",
        field_delimiter=",",
    )

    extract_session_to_gcs = PostgresToGCSOperator(
        task_id="extract_session_to_gcs",
        postgres_conn_id="postgres_con_gcs",
        sql="SELECT * FROM ecommerce.sessions",
        bucket="jdeol003-bucket",
        filename="final_project_de03_dimasadihartomo/master/session_stg.csv",
        export_format="csv",
        field_delimiter=",",
    )

    extract_transaction_to_gcs = PostgresToGCSOperator(
        task_id="extract_transaction_to_gcs",
        postgres_conn_id="postgres_con_gcs",
        sql="SELECT * FROM ecommerce.transaction WHERE DATE(created_at) = '{{ yesterday_ds  }}'",
        bucket="jdeol003-bucket",
        filename="final_project_de03_dimasadihartomo/transaction/transaction_{{ yesterday_ds }}.csv",
        export_format="csv",
        field_delimiter=",",
    )

    extract_review_to_gcs = PostgresToGCSOperator(
        task_id="extract_review_to_gcs",
        postgres_conn_id="postgres_con_gcs",
        sql="SELECT * FROM ecommerce.review WHERE DATE(created_at) = '{{ yesterday_ds  }}'",
        bucket="jdeol003-bucket",
        filename="final_project_de03_dimasadihartomo/review/review_{{ yesterday_ds }}.csv",
        export_format="csv",
        field_delimiter=",",
    )

    [
        extract_customer_to_gcs,
        extract_product_to_gcs,
        extract_payment_to_gcs,
        extract_session_to_gcs,
    ] >> extract_transaction_to_gcs >> extract_review_to_gcs