import os
import json
import requests
from datetime import datetime
import subprocess
import time
from abc import ABC, abstractmethod

class ApiHandler(ABC):
    def __init__(self, config):
        self.api_url = config['api']['url']
        self.api_endpoint = config['api']['endpoint']
        self.api_key = config['api'].get('key')
        self.partition_name = config['partition']['name']
        self.local_dir = config['local']['dir']
        self.hdfs_dir = config['hdfs']['dir']
        self.extra_params = json.loads(config['api'].get('extra_params', '{}'))

    @abstractmethod
    def fetch_api(self, update_time=None):
        pass

    def ensure_local_path(self, path):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def save_json_local(self, data, partition_date):
        dt = datetime.strptime(partition_date, "%Y-%m-%d") if partition_date else datetime.now()
        path = os.path.join(self.local_dir, f"{self.partition_name}/yyyy={dt.year}/mm={dt.month:02d}/dd={dt.day:02d}")
        self.ensure_local_path(path)
        file_path = os.path.join(path, "data.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return file_path, dt

    def ensure_hdfs_path(self, hdfs_path):
        parent = os.path.dirname(hdfs_path)
        cmd = ["hdfs", "dfs", "-mkdir", "-p", parent]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0 and "File exists" not in result.stderr:
            print(f"HDFS mkdir failed: {result.stderr}")

    def put_to_hdfs(self, local_path, dt):
        hdfs_path = f"{self.hdfs_dir}/{self.partition_name}/yyyy={dt.year}/mm={dt.month:02d}/dd={dt.day:02d}/data.json"
        self.ensure_hdfs_path(hdfs_path)
        cmd = ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"HDFS put failed: {result.stderr}")
            return False
        print(f"Uploaded to HDFS: {hdfs_path}")
        return True
