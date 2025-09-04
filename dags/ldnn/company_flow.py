from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'a25bnv',
    'depends_on_past': False,
    'retries': 1,
    'email_on_failure': False,
}

PROJECT_ROOT = '/home/hadoop/a25bnv-dataflowcommon'

dag = DAG(
    dag_id='ldnn_company_to_hdfs',
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['api', 'hdfs'],
    params={
        'update_time': "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    }
)

step1_command = (
    f"python {PROJECT_ROOT}/tasks/fetch/ldnn/company_fetch.py "
    f"--config {PROJECT_ROOT}/configuration/fetch/ldnn/company.toml "
    "--update-time {{ params.update_time }}"
)

step1_task = BashOperator(
    task_id='ldnn_company_to_hdfs',
    bash_command=step1_command,
    dag=dag,
)

