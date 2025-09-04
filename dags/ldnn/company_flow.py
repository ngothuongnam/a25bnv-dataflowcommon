from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import subprocess

PROJECT_ROOT = '/home/hadoop/a25bnv-dataflowcommon'

# Fetch API task
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

# Load staging task
def run_company_load_staging(**context):
    update_time = context['params']['update_time']
    mapping_config = os.path.join(PROJECT_ROOT, 'configuration', 'load_staging', 'ldnn', 'company.toml')
    dt = datetime.strptime(update_time, "%Y-%m-%d")
    json_path = f"/user/hadoop/api/ldnn/company/yyyy={dt.year}/mm={dt.month:02d}/dd={dt.day:02d}/data.json"
    script_path = os.path.join(PROJECT_ROOT, 'tasks', 'load_staging', 'ldnn', 'company_load_staging.py')
    cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        script_path,
        "--json-path", json_path,
        "--update-time", update_time,
        "--config", mapping_config
    ]
    print("[DEBUG] spark-submit command:", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Staging load failed.\nStdout:\n{result.stdout}\nStderr:\n{result.stderr}")


default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'retries': 0,
    'email_on_failure': False,
}

dag = DAG(
    dag_id='ldnn_company_flow',
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

step2_task = PythonOperator(
    task_id='ldnn_company_load_staging',
    python_callable=run_company_load_staging,
    provide_context=True,
    dag=dag,
)

step1_task >> step2_task

