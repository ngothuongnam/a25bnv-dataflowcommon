# a25bnv-dataflowcommon

ETL Pipeline dùng Airflow, Spark, HDFS và SQL Server với kiến trúc OOP và cấu hình động qua TOML.

## Tổng quan

Pipeline này hỗ trợ xử lý dữ liệu từ API public về HDFS, load staging qua Spark/Hive, và đổ vào SQL Server làm data mart. Thiết kế với kiến trúc 3 tầng kế thừa để dễ mở rộng cho nhiều hệ thống và domain khác nhau.

## Cấu trúc Project

```
a25bnv-dataflowcommon/
├── common/                     # Các class chung, tái sử dụng
│   ├── fetch/                  # Logic fetch API
│   │   ├── api_handler.py      # Base class cho tất cả API handlers
│   │   └── ldnn/               # Domain LDNN
│   │       ├── ldnn_handler.py # LDNN handler chung (kế thừa ApiHandler)
│   │       └── company_handler.py # Company handler (kế thừa LdnnHandler)
│   ├── load_staging/           # Logic load vào Hive staging
│   │   ├── base_staging_loader.py # Base class cho staging loaders
│   │   └── ldnn/
│   │       └── company_staging_loader.py # Company staging loader
│   └── load_mart/              # Logic load vào SQL Server
│       ├── base_mart_loader.py # Base class cho mart loaders
│       └── ldnn/
│           └── company_mart_loader.py # Company mart loader
├── configuration/              # File cấu hình TOML
│   ├── fetch/ldnn/company.toml # Config API, local, HDFS paths
│   ├── load_staging/ldnn/company.toml # Config field mapping
│   └── load_mart/ldnn/company.toml # Config JDBC, table, columns
├── tasks/                      # Script thực thi từng bước
│   ├── fetch/ldnn/company_fetch.py # Fetch API → HDFS
│   ├── load_staging/ldnn/company_load_staging.py # HDFS → Hive staging
│   └── load_mart/ldnn/company_load_mart.py # Staging → SQL Server
└── dags/                       # Airflow DAGs
    └── ldnn/company_flow.py    # Pipeline 3 bước: fetch → staging → mart
```

## Kiến trúc

### 1. Fetch Layer (API → HDFS)
- **ApiHandler**: Base class chứa `__init__(config)`, abstract method `fetch_api` và logic handle API chung
- **LdnnHandler**: Kế thừa ApiHandler override fetch_api để xử lý logic cho nguồn LDNN
- **CompanyHandler**: Kế thừa LdnnHandler, implement hàm `run()` riêng cho company

### 2. Load Staging Layer (HDFS → Hive)
- **BaseStagingLoader**: Base class với logic đọc JSON, áp dụng mapping, ghi vào Hive
- **CompanyStagingLoader**: Kế thừa BaseStagingLoader, chỉ định table và config

### 3. Load Mart Layer (Staging → SQL Server)
- **BaseMartLoader**: Base class với logic đọc từ staging, ghi qua JDBC
- **CompanyMartLoader**: Kế thừa BaseMartLoader, chỉ định table và config

### 4. Orchestration (Airflow)
- **company_flow.py**: DAG 3 task tuần tự với param `update_time` động

## Luồng dữ liệu

```
API (LDNN) → JSON (Local) → HDFS (Partitioned) → Hive Staging → SQL Server Mart
```

1. **Fetch**: Gọi API LDNN, lưu JSON local, upload HDFS theo partition yyyy/mm/dd
2. **Load Staging**: Đọc JSON từ HDFS, áp dụng field mapping, ghi vào bảng Hive staging với partition update_time
3. **Load Mart**: Đọc từ staging, transform và ghi vào bảng SQL Server mart

## Cấu hình

### Fetch Config (configuration/fetch/ldnn/company.toml)
```toml
[api]
url = "https://api.example.com"
endpoint = "/companies"
key = "your_api_key"

[local]
dir = "/tmp/data"

[hdfs]
dir = "/user/hadoop/api"

[partition]
name = "ldnn/company"
```

### Staging Config (configuration/load_staging/ldnn/company.toml)
```toml
[field_mapping]
staging_field = "api_field"
company_name = "tenCongTy"
```

### Mart Config (configuration/load_mart/ldnn/company.toml)
```toml
[mart]
table = "ldnn.company"
columns = ["id", "tenCongTy", "update_time"]
jdbc_url = "jdbc:sqlserver://server:1433;databaseName=db;trustServerCertificate=true"
jdbc_properties = { user = "user", password = "pass", driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver" }
```

## Chạy Pipeline

### Manual (qua Airflow UI)
1. Trigger DAG `ldnn_company_flow`
2. Truyền param `update_time` (format: YYYY-MM-DD)

### Programmatic
```bash
# Fetch only
python tasks/fetch/ldnn/company_fetch.py --config configuration/fetch/ldnn/company.toml --update-time 2025-09-05

# Load staging only
spark-submit tasks/load_staging/ldnn/company_load_staging.py --config configuration/load_staging/ldnn/company.toml --json-path /user/hadoop/api/ldnn/company/yyyy=2025/mm=09/dd=05/data.json --update-time 2025-09-05

# Load mart only
spark-submit tasks/load_mart/ldnn/company_load_mart.py --config configuration/load_mart/ldnn/company.toml --json-path /user/hadoop/api/ldnn/company/yyyy=2025/mm=09/dd=05/data.json --update-time 2025-09-05
```

## Mở rộng

### Thêm domain mới (ví dụ: tax)
1. Tạo thư mục `common/fetch/tax/`, `tasks/fetch/tax/`, `configuration/fetch/tax/`
2. Tạo class `TaxHandler` kế thừa `ApiHandler`
3. Tạo class `TaxStagingLoader` kế thừa `BaseStagingLoader`
4. Tạo class `TaxMartLoader` kế thừa `BaseMartLoader`
5. Tạo các script task và DAG tương ứng

### Thêm entity mới trong LDNN (ví dụ: person)
1. Tạo `PersonHandler` kế thừa `LdnnHandler`
2. Tạo `PersonStagingLoader` kế thừa `BaseStagingLoader`
3. Tạo `PersonMartLoader` kế thừa `BaseMartLoader`
4. Tạo config, task script và DAG

## Yêu cầu

- Python 3.8+
- Apache Airflow 2.x
- Apache Spark 3.x
- Hadoop HDFS
- SQL Server + JDBC Driver
- Python packages: `tomli`, `requests`, `pyspark`, `pyodbc`

## Lưu ý

- File JSON không tồn tại sẽ skip task, không fail pipeline
- SparkSession được auto cleanup trong `finally` block
- Partition staging theo `update_time`, mart không partition
- Hỗ trợ SSL bypass với `trustServerCertificate=true`
