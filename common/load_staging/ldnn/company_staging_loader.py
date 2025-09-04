from common.load_staging.base_staging_loader import BaseStagingLoader

class LDNNCompanyStagingLoader(BaseStagingLoader):
    def __init__(self, config_path):
        super().__init__(table_name="staging.company", config_path=config_path)

