import sys
import argparse
from common.load_staging.ldnn.company_staging_loader import LDNNCompanyStagingLoader

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load LDNN company JSON to staging table")
    parser.add_argument('--json-path', type=str, required=True, help='Path to JSON file')
    parser.add_argument('--update-time', type=str, required=True, help='Update time (YYYY-MM-DD)')
    args = parser.parse_args()

    loader = LDNNCompanyStagingLoader()
    loader.load_json(args.json_path, args.update_time)
    loader.stop()
