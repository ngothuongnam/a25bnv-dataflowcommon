from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import subprocess

PROJECT_ROOT = '/home/hadoop/a25bnv-dataflowcommon'

# Fetch API task
def run_company_etl(**context):
    update_time = context['params'].get('update_time')
    config_path = os.path.join(PROJECT_ROOT, 'configuration', 'fetch', 'ldnn', 'company.toml')
    script_path = os.path.join(PROJECT_ROOT, 'tasks', 'fetch', 'ldnn', 'company_fetch.py')
    python_bin = "/home/hadoop/miniconda3/envs/hadoop/bin/python3"
    cmd = f"{python_bin} {script_path} --config {config_path} --update-time {update_time}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        raise Exception(f"ETL script failed.\nStdout:\n{result.stdout}\nStderr:\n{result.stderr}")

# Load staging task
def run_company_load_staging(**context):
    update_time = context['params'].get('update_time')
    mapping_config = os.path.join(PROJECT_ROOT, 'configuration', 'load_staging', 'ldnn', 'company.toml')
    dt = datetime.strptime(update_time, "%Y-%m-%d")
    json_path = f"/user/hadoop/api/ldnn/company/yyyy={dt.year}/mm={dt.month:02d}/dd={dt.day:02d}/data.json"
    script_path = os.path.join(PROJECT_ROOT, 'tasks', 'load_staging', 'ldnn', 'company_load_staging.py')
    python_bin = "/home/hadoop/miniconda3/envs/hadoop/bin/python3"
    cmd = f"/home/hadoop/spark-3.3.1/bin/spark-submit --master yarn --deploy-mode client --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON={python_bin} --conf spark.executorEnv.PYSPARK_PYTHON={python_bin} {script_path} --json-path {json_path} --update-time {update_time} --config {mapping_config}"
    print("[DEBUG] spark-submit command:", cmd)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
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
        'update_time': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
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

