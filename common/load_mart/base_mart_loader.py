from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import tomli
import subprocess
import json

class BaseMartLoader:
    def __init__(self, table, config_path):
        self.table = table
        self.spark = SparkSession.builder.appName("MartLoader").getOrCreate()
        if not config_path:
            raise ValueError("config_path is required for BaseMartLoader")
        with open(config_path, 'rb') as f:
            config = tomli.load(f)
        self.columns = [col for col in config['mart']['columns'] if col.lower() != 'pk_id']
        self.jdbc_url = config['mart']['jdbc_url']
        self.jdbc_properties = config['mart']['jdbc_properties']

    def load_json(self, json_path, update_time=None):
        check_cmd = ["hdfs", "dfs", "-test", "-e", json_path]
        result = subprocess.run(check_cmd)
        if result.returncode != 0:
            print(f"File {json_path} not found on HDFS. Skipping mart load.")
            return
        df = self.spark.read.option("multiLine", True).json(json_path)
        if update_time:
            update_time_str = update_time.replace("-", "")
            df = df.withColumn("update_time", lit(update_time_str))
        df = df.select(*self.columns)
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", self.table) \
            .option("user", self.jdbc_properties['user']) \
            .option("password", self.jdbc_properties['password']) \
            .option("driver", self.jdbc_properties['driver']) \
            .mode("append") \
            .save()

    def stop(self):
        self.spark.stop()