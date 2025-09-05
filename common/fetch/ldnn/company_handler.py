from common.fetch.ldnn.ldnn_handler import LdnnHandler

class CompanyHandler(LdnnHandler):
    def __init__(self, config):
        super().__init__(config)

    def run(self, update_time):
        data = self.fetch_api(update_time)
        if not data:
            print("No data fetched.")
            return False
        local_path, dt = self.save_json_local(data, update_time)
        print(f"Saved: {local_path}")
        return self.put_to_hdfs(local_path, dt)
