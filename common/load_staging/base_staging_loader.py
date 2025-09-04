from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import tomli
import os

class BaseStagingLoader:
    def __init__(self, table_name):
        self.table_name = table_name
        self.spark = SparkSession.builder.appName("StagingLoader").enableHiveSupport().getOrCreate()
        # Đọc mapping từ file cấu hình cố định
        config_path = os.path.join(os.getcwd(), 'configuration', 'load_staging', 'ldnn', 'company.toml')
        with open(config_path, 'rb') as f:
            config = tomli.load(f)
        self.mapping = config.get('field_mapping')

    def load_json(self, json_path, update_time):
        update_time_str = update_time.replace("-", "")
        df = self.spark.read.json(json_path)
        df = df.withColumn("update_time", lit(update_time_str))
        if self.mapping:
            df = self.apply_mapping(df)
        self.write_to_staging(df)

    def apply_mapping(self, df):
        # mapping: {"staging_field": "api_field", ...}
        select_expr = [f"{api_field} as {staging_field}" for staging_field, api_field in self.mapping.items()]
        return df.selectExpr(*select_expr)

    def write_to_staging(self, df):
        df.write.mode("overwrite").format("parquet").partitionBy("update_time").saveAsTable(self.table_name)

    def stop(self):
        self.spark.stop()
