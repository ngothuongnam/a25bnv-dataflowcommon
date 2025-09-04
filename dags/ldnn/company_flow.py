from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import subprocess

PROJECT_ROOT = '/home/hadoop/a25bnv-dataflowcommon'

# Hàm thực thi ETL
def run_company_etl(**context):
    update_time = context['params']['update_time']
    config_path = os.path.join(PROJECT_ROOT, 'configuration', 'fetch', 'ldnn', 'company.toml')
    script_path = os.path.join(PROJECT_ROOT, 'tasks', 'fetch', 'ldnn', 'company_fetch.py')
    cmd = [sys.executable, script_path, '--config', config_path, '--update-time', update_time]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        raise Exception(f"ETL script failed.\nStdout:\n{result.stdout}\nStderr:\n{result.stderr}")


default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'retries': 0,
    'email_on_failure': False,
}

dag = DAG(
    dag_id='ldnn_company_to_hdfs',
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=['ldnn', 'company'],
    params={
        'update_time': "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    }
)

step1_task = PythonOperator(
    task_id='ldnn_company_to_hdfs',
    python_callable=run_company_etl,
    provide_context=True,
    dag=dag,
)

