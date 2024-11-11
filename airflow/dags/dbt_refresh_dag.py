from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag_id = 'dbt_refresh_job_analysis_v1'

with DAG(
    dag_id,
    default_args=default_args,
    description='Refresh dbt models every 12 hours',
    schedule_interval='0 */12 * * *',
    catchup=False,
    tags=['dbt'],
) as dag:

    create_log_dir = BashOperator(
        task_id='create_log_dir',
        bash_command='mkdir -p /opt/airflow/dbt_project/logs',
    )

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt_project && echo "Running dbt deps..." && dbt deps --profiles-dir /opt/airflow/.dbt',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && echo "Running dbt run..." && dbt run --profiles-dir /opt/airflow/.dbt',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && echo "Running dbt test..." && dbt test --profiles-dir /opt/airflow/.dbt',
    )

    create_log_dir >> dbt_deps >> dbt_run >> dbt_test
