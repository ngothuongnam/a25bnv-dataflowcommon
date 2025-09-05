from common.load_mart.base_mart_loader import BaseMartLoader

class CompanyMartLoader(BaseMartLoader):
    def __init__(self, config):
        super().__init__(table=config['mart']['table'], config=config)
