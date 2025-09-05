from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

PROJECT_ROOT = '/home/hadoop/a25bnv-dataflowcommon'
PYTHON_BIN = "/home/hadoop/miniconda3/envs/hadoop/bin/python3"
SPARK_SUBMIT = "/home/hadoop/spark-3.3.1/bin/spark-submit"

# Helper get update_time
def get_update_time(context):
    update_time = context['params'].get('update_time')
    if not update_time or '{{' in str(update_time):
        execution_date = context['execution_date']
        update_time = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    return update_time

# Task generic
def run_script(task_type, **context):
    update_time = get_update_time(context)
    dt = datetime.strptime(update_time, "%Y-%m-%d")
    json_path = f"/user/hadoop/api/ldnn/company/yyyy={dt.year}/mm={dt.month:02d}/dd={dt.day:02d}/data.json"
    config_map = {
        'fetch': os.path.join(PROJECT_ROOT, 'configuration', 'fetch', 'ldnn', 'company.toml'),
        'staging': os.path.join(PROJECT_ROOT, 'configuration', 'load_staging', 'ldnn', 'company.toml'),
        'mart': os.path.join(PROJECT_ROOT, 'configuration', 'load_mart', 'ldnn', 'company.toml'),
    }
    script_map = {
        'fetch': os.path.join(PROJECT_ROOT, 'tasks', 'fetch', 'ldnn', 'company_fetch.py'),
        'staging': os.path.join(PROJECT_ROOT, 'tasks', 'load_staging', 'ldnn', 'company_load_staging.py'),
        'mart': os.path.join(PROJECT_ROOT, 'tasks', 'load_mart', 'ldnn', 'company_load_mart.py'),
    }
    config_path = config_map[task_type]
    script_path = script_map[task_type]
    if task_type == 'fetch':
        cmd = f"{PYTHON_BIN} {script_path} --config {config_path} --update-time {update_time}"
    else:
        cmd = f"{SPARK_SUBMIT} --master yarn --deploy-mode client --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON={PYTHON_BIN} --conf spark.executorEnv.PYSPARK_PYTHON={PYTHON_BIN} {script_path} --json-path {json_path} --update-time {update_time} --config {config_path}"
    print(f"[DEBUG] {task_type} command:", cmd)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        raise Exception(f"{task_type.capitalize()} task failed.\nStdout:\n{result.stdout}\nStderr:\n{result.stderr}")

# Airflow DAG

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
    python_callable=lambda **context: run_script('fetch', **context),
    provide_context=True,
    dag=dag,
)

step2_task = PythonOperator(
    task_id='ldnn_company_load_staging',
    python_callable=lambda **context: run_script('staging', **context),
    provide_context=True,
    dag=dag,
)

step3_task = PythonOperator(
    task_id='ldnn_company_load_mart',
    python_callable=lambda **context: run_script('mart', **context),
    provide_context=True,
    dag=dag,
)

step1_task >> step2_task >> step3_task

