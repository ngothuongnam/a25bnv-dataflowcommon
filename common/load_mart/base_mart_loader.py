from pyspark.sql import SparkSession
import subprocess
import json

class BaseMartLoader:
    def __init__(self, table, config):
        self.table = table
        self.columns = [col for col in config['mart']['columns'] if col.lower() != 'pk_id']
        self.jdbc_url = config['mart']['jdbc_url']
        self.jdbc_properties = config['mart']['jdbc_properties']
        self.spark = SparkSession.builder.appName("MartLoader").getOrCreate()

    def load(self, data_path: str):
        # Kiểm tra file json trên HDFS
        check_cmd = ["hdfs", "dfs", "-test", "-e", data_path]
        result = subprocess.run(check_cmd)
        if result.returncode != 0:
            print(f"File {data_path} not found on HDFS. Skipping mart load.")
            return
        df = self.spark.read.option("multiLine", True).json(data_path)
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