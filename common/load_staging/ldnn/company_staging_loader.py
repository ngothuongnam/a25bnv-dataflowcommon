from common.load_staging.base_staging_loader import BaseStagingLoader

class LDNNCompanyStagingLoader(BaseStagingLoader):
    def __init__(self):
        super().__init__(table_name="staging.company")

    # Nếu cần custom thêm logic cho nguồn LDNN, override các phương thức ở đây
