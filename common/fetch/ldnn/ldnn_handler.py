import os
import json
import requests
from datetime import datetime
import subprocess
import time
from common.fetch.api_handler import ApiHandler

class LdnnHandler(ApiHandler):
    def __init__(self, config):
        super().__init__(config)

    def fetch_api(self, update_time=None):
        url = f"{self.api_url}{self.api_endpoint}"
        headers = {'DolabApiKey': self.api_key} if self.api_key else {}
        params = self.extra_params.copy() if self.extra_params else {}
        if update_time:
            params['updateTime'] = update_time
        items = []
        next_cursor = None
        while True:
            if next_cursor:
                params['nextCursor'] = next_cursor
                time.sleep(1)
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data.get('isSucceeded'):
                break
            page_items = data.get('data', {}).get('items', [])
            items.extend(page_items)
            next_cursor = data.get('data', {}).get('nextCursor')
            if not next_cursor:
                break
        return items