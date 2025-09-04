import sys
import os
import tomli
from common.fetch.ldnn.company_handler import LDNNCompanyHandler
from datetime import datetime

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch LDNN company API, save JSON, put to HDFS")
    parser.add_argument('--config', type=str, required=True, help='Path to config toml file')
    parser.add_argument('--update-time', type=str, required=True, help='API updateTime param (YYYY-MM-DD)')
    args = parser.parse_args()

    with open(args.config, 'rb') as f:
        config = tomli.load(f)
    handler = LDNNCompanyHandler(config)
    success = handler.run(update_time=args.update_time)
    if not success:
        print("ETL task finished but failed (not uploaded to HDFS or no data)")
