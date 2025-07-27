from airflow.utils.email import send_email_smtp

def failed_notification(context):
    task_instance = context["ti"]
    exception = context.get("exception", "Unknown error")

    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context["execution_date"]
    log_url = task_instance.log_url

    subject = f"[Airflow] Task Failed: {task_id} in DAG {dag_id}"
    html_content = f"""
        <h3>Airflow Task Failed</h3>
        <p><strong>DAG:</strong> {dag_id}</p>
        <p><strong>Task:</strong> {task_id}</p>
        <p><strong>Execution Time:</strong> {execution_date}</p>
        <p><strong>Error:</strong> {exception}</p>
        <p><a href="{log_url}">View Logs</a></p>
    """

    send_email_smtp(
        to="dimasadi1308@gmail.com",
        subject=subject,
        html_content=html_content
    )