from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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

files_to_load = [

    {
        "gcs_path": "final_project_de03_dimasadihartomo/master/customer_stg.csv",
        "table": "customer_stg"
    },

    {
        "gcs_path": "final_project_de03_dimasadihartomo/master/product_stg.csv",
        "table": "product_stg"
    },

        {
        "gcs_path": "final_project_de03_dimasadihartomo/master/payment_stg.csv",
        "table": "payment_stg"
    },

    {
        "gcs_path": "final_project_de03_dimasadihartomo/master/session_stg.csv",
        "table": "session_stg"
    },

    {
        "gcs_path": "final_project_de03_dimasadihartomo/transaction/transaction_{{ yesterday_ds }}.csv",
        "table": "transaction_stg"
    },
    {
        "gcs_path": "final_project_de03_dimasadihartomo/review/review_{{ yesterday_ds }}.csv",
        "table": "review_stg"
    },
]

with DAG(
    dag_id='load_gcs_to_bq',
    default_args=default_args,
    schedule="30 1 * * *",
    catchup=False,
    tags=['gcs', 'load', 'daily', 'bigquery'],
    description="DAG to load CSV files from GCS to Bigquery"
) as dag:
    
    start = EmptyOperator(task_id="start_load")
    end = EmptyOperator(task_id="end_load")
    
    # wait_for_extract_dag_success = ExternalTaskSensor(
    #     task_id='wait_for_extract_dag_success',
    #     external_dag_id='extract_postgres_to_gcs',
    #     external_task_id='extract_review_to_gcs',
    #     allowed_states=['success'],
    #     failed_states=['failed', 'skipped'],
    #     poke_interval=300,
    #     timeout=3600,
    #     mode='poke',
    #     execution_delta=timedelta(minutes=30)
    # )
    
    for file_cfg in files_to_load:

        is_transactional = "{{ yesterday_ds }}" in file_cfg["gcs_path"]

        write_disposition = "WRITE_APPEND" if is_transactional else "WRITE_TRUNCATE"

        task_id = f"load_{file_cfg['table']}_csv"
            
                
        load_task = GCSToBigQueryOperator(
            task_id=task_id,
            bucket='jdeol003-bucket',
            source_objects=[file_cfg["gcs_path"]],
            destination_project_dataset_table=f"purwadika.jcdeol03_finalproject_dimasadihartomo.{file_cfg['table']}",
            source_format="CSV",
            write_disposition=write_disposition,
            skip_leading_rows=1,
            autodetect=True,
            time_partitioning={
                "type": "DAY",
                "field": "created_at"
            },
        )

        # wait_for_extract_dag_success >> start >> load_task >> end
        if is_transactional:
            delete_partition = BigQueryInsertJobOperator(
                task_id=f"delete_partition_{file_cfg['table']}",
                configuration={
                    "query": {
                        "query": f"""
                            IF EXISTS (
                                SELECT 1
                                FROM `purwadika.jcdeol03_finalproject_dimasadihartomo`.INFORMATION_SCHEMA.TABLES
                                WHERE table_name = '{file_cfg['table']}'
                            ) THEN
                                DELETE FROM `purwadika.jcdeol03_finalproject_dimasadihartomo.{file_cfg['table']}`
                                WHERE DATE(created_at) = DATE('{{{{ yesterday_ds }}}}');
                            END IF;
                        """,
                        "useLegacySql": False,
                    }
                }
            )

            start >> delete_partition >> load_task >> end
        else:
            start >> load_task >> end