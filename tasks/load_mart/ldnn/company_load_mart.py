import tomli
import argparse
from common.load_mart.ldnn.company_mart_loader import CompanyMartLoader
from datetime import datetime
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--json-path', required=True)
    parser.add_argument('--update-time', required=False)
    args = parser.parse_args()

    with open(args.config, "rb") as f:
        config = tomli.load(f)

    # Cơ chế lấy update_time giống load_staging
    update_time = args.update_time
    if not update_time:
        update_time = datetime.now().strftime('%Y-%m-%d')
    dt = datetime.strptime(update_time, "%Y-%m-%d")
    json_path = args.json_path
    if not os.path.exists(json_path):
        print(f"File {json_path} not found.")
        exit(1)

    loader = CompanyMartLoader(config)
    loader.load(json_path)
