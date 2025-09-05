import tomli
import argparse
from common.load_mart.ldnn.company_mart_loader import CompanyMartLoader
from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--json-path', required=True)
    parser.add_argument('--update-time', required=False)
    args = parser.parse_args()

    with open(args.config, "rb") as f:
        config = tomli.load(f)

    update_time = args.update_time or datetime.now().strftime('%Y-%m-%d')
    loader = CompanyMartLoader(config)
    result = loader.load_json(args.json_path, update_time)
    if result is None:
        print("Mart load skipped: file not found.")
