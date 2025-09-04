from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import tomli
import os
import subprocess

class BaseStagingLoader:
    def __init__(self, table_name, config_path):
        self.table_name = table_name
        self.spark = SparkSession.builder.appName("StagingLoader").enableHiveSupport().getOrCreate()
        if not config_path:
            raise ValueError("config_path is required for BaseStagingLoader")
        with open(config_path, 'rb') as f:
            config = tomli.load(f)
        self.mapping = config.get('field_mapping')

    def load_json(self, json_path, update_time):
        check_cmd = ["hdfs", "dfs", "-test", "-e", json_path]
        result = subprocess.run(check_cmd)
        if result.returncode != 0:
            print(f"File {json_path} not found on HDFS. Skipping staging load.")
            return
        update_time_str = update_time.replace("-", "")
        df = self.spark.read.option("multiLine", True).json(json_path)
        if self.mapping:
            df = self.apply_mapping(df)
        df = df.withColumn("update_time", lit(update_time_str))
        self.write_to_staging(df)

    def apply_mapping(self, df):
        select_expr = [f"{api_field} as {staging_field}" for staging_field, api_field in self.mapping.items()]
        return df.selectExpr(*select_expr)

    def write_to_staging(self, df):
        # Drop old partition if exists
        update_time_values = df.select("update_time").distinct().rdd.flatMap(lambda x: x).collect()
        for update_time_str in update_time_values:
            self.spark.sql(f"ALTER TABLE {self.table_name} DROP IF EXISTS PARTITION (update_time='{update_time_str}')")
        # Append new data to partition
        df.write.mode("append").format("parquet").partitionBy("update_time").saveAsTable(self.table_name)

    def stop(self):
        self.spark.stop()
