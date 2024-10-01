from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from airflow.models.baseoperator import chain
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
import os


def traverse_directory(directory, file_extension):
    local_file_path = []
    file_name = []
    partition_dir = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))  # Loại bỏ thư mục gốc
                local_file_path.append(file_path)

    return file_name, partition_dir, local_file_path

@dag(
    start_date=datetime(2024, 9, 14),
    schedule_interval=None,
    catchup=False,
    tags=['mobile']
)
def mobile():
    directory_path = '/usr/local/airflow/include/dataset/'
    
    # Duyệt qua thư mục để lấy danh sách tệp
    csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(directory_path, '.csv')
    parquet_file_name, parquet_partition_dir, parquet_local_file_path = traverse_directory(directory_path, '.parquet')
    json_file_name, json_partition_dir, json_local_file_path = traverse_directory(directory_path, '.json')

    # Upload các tệp CSV lên GCS
    upload_csv_tasks = []
    for csv_index, file_element in enumerate(csv_file_name):
        upload_csv_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f'upload_csv_{csv_index}',
            src=csv_local_file_path[csv_index],  # Đường dẫn tệp cục bộ
            dst=f'{csv_partition_dir[csv_index]}/{file_element}',  # Đường dẫn tệp trong GCS
            bucket='dbt-hieu-bucket',  # Tên bucket,
            gcp_conn_id='gcp',
            mime_type='text/csv'  # Explicitly setting MIME type to text/csv

        )
        upload_csv_tasks.append(upload_csv_to_gcs)
        
    upload_exchange_rate_csv_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_exchange_csv',
            src='/usr/local/airflow/include/dataset/exchange_rates.csv',  # Đường dẫn tệp cục bộ
            dst='rate_exchange.csv',  # Đường dẫn tệp trong GCS
            bucket='dbt-hieu-bucket',  # Tên bucket,
            gcp_conn_id='gcp',
            mime_type='text/csv'  # Explicitly setting MIME type to text/csv

        )
    
    # Upload các tệp Parquet lên GCS
    upload_parquet_tasks = []
    for parquet_index, file_element in enumerate(parquet_file_name):
        upload_parquet_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f'upload_parquet_{parquet_index}',
            src=parquet_local_file_path[parquet_index],  # Đường dẫn tệp cục bộ
            dst=f'{parquet_partition_dir[parquet_index]}/{file_element}',  # Đường dẫn tệp trong GCS
            bucket='dbt-hieu-bucket',  # Tên bucket
            gcp_conn_id='gcp'
        )
        upload_parquet_tasks.append(upload_parquet_to_gcs)
   
    # Upload các tệp JSON lên GCS
    upload_json_tasks = []
    for json_index, file_element in enumerate(json_file_name):
        upload_json_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f'upload_json_{json_index}',
            src=json_local_file_path[json_index],  # Đường dẫn tệp cục bộ
            dst=f'{json_partition_dir[json_index]}/{file_element}',  # Đường dẫn tệp trong GCS
            bucket='dbt-hieu-bucket',  # Tên bucket
            gcp_conn_id='gcp'
        )
        upload_json_tasks.append(upload_json_to_gcs)
     
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='Mobile',  # Dataset bạn muốn tạo
        gcp_conn_id='gcp'
    )
    
    
   
    create_external_table = BigQueryCreateExternalTableOperator(
    task_id='create_csv_external_table',
    destination_project_dataset_table='data-435516.Mobile.staging_csv_data',
    bucket='dbt-hieu-bucket',
    source_objects=['source=IN/format=csv/*.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    field_delimiter=',',
    allow_quoted_newlines=True,
    schema_fields=[
        {"name": "Order_ID", "type": "STRING"},
        {"name": "Customer_Name", "type": "STRING"},
        {"name": "Mobile_Model", "type": "STRING"},
        {"name": "Quantity", "type": "INT64"},
        {"name": "Price_per_Unit", "type": "INT64"},
        {"name": "Total_Price", "type": "INT64"},
        {"name": "Promotion_Code", "type": "STRING"},
        {"name": "Order_Amount", "type": "FLOAT64"},
        {"name": "Tax", "type": "FLOAT64"},
        {"name": "Order_Date", "type": "DATE"},
        {"name": "Payment_Status", "type": "STRING"},
        {"name": "Shipping_Status", "type": "STRING"},
        {"name": "Payment_Method", "type": "STRING"},
        {"name": "Payment_Provider", "type": "STRING"},
        {"name": "Phone", "type": "STRING"},
        {"name": "Delivery_Address", "type": "STRING"}
    ],
    location='US',
    gcp_conn_id='gcp'
    )
    
    create_external_table_json = BigQueryCreateExternalTableOperator(
    task_id='create_json_external_table',
    destination_project_dataset_table='data-435516.Mobile.staging_json_data',
    bucket='dbt-hieu-bucket',
    source_objects=['source=FR/format=json/*.json'],
    source_format='NEWLINE_DELIMITED_JSON',
    schema_fields=[
        {"name": "Order_ID", "type": "STRING"},
        {"name": "Customer_Name", "type": "STRING"},
        {"name": "Mobile_Model", "type": "STRING"},
        {"name": "Quantity", "type": "INT64"},
        {"name": "Price_per_Unit", "type": "INT64"},
        {"name": "Total_Price", "type": "INT64"},
        {"name": "Promotion_Code", "type": "STRING"},
        {"name": "Order_Amount", "type": "FLOAT64"},
        {"name": "Tax", "type": "FLOAT64"},
        {"name": "Order_Date", "type": "DATE"},
        {"name": "Payment_Status", "type": "STRING"},
        {"name": "Shipping_Status", "type": "STRING"},
        {"name": "Payment_Method", "type": "STRING"},
        {"name": "Payment_Provider", "type": "STRING"},
        {"name": "Phone", "type": "STRING"},
        {"name": "Delivery_Address", "type": "STRING"}
    ],
    location='US',
    gcp_conn_id='gcp'
    )

    create_external_table_parquet = BigQueryCreateExternalTableOperator(
    task_id='create_parquet_external_table',
    destination_project_dataset_table='data-435516.Mobile.staging_parquet_data',
    bucket='dbt-hieu-bucket',
    source_objects=['source=US/format=parquet/*.parquet'],
    source_format='PARQUET',
    schema_fields=[
        {"name": "Order_ID", "type": "STRING"},
        {"name": "Customer_Name", "type": "STRING"},
        {"name": "Mobile_Model", "type": "STRING"},
        {"name": "Quantity", "type": "INT64"},
        {"name": "Price_per_Unit", "type": "INT64"},
        {"name": "Total_Price", "type": "INT64"},
        {"name": "Promotion_Code", "type": "STRING"},
        {"name": "Order_Amount", "type": "FLOAT64"},
        {"name": "Tax", "type": "FLOAT64"},
        {"name": "Order_Date", "type": "STRING"},
        {"name": "Payment_Status", "type": "STRING"},
        {"name": "Shipping_Status", "type": "STRING"},
        {"name": "Payment_Method", "type": "STRING"},
        {"name": "Payment_Provider", "type": "STRING"},
        {"name": "Phone", "type": "STRING"},
        {"name": "Delivery_Address", "type": "STRING"}
    ],
    location='US',
    gcp_conn_id='gcp'
    )
    
    create_external_table_exchange_rate = BigQueryInsertJobOperator(
    task_id='create_exchange_rate_external_table',
    configuration={
        "query": {
            "query": """
                CREATE OR REPLACE EXTERNAL TABLE data-435516.Mobile.rate_exchange_data
                OPTIONS (
                  format = 'CSV',
                  uris = ['gs://dbt-hieu-bucket/rate_exchange.csv'],
                  skip_leading_rows = 1,
                  field_delimiter = ','
                )
            """,
            "useLegacySql": False
        }
    },
    location='US',
    gcp_conn_id='gcp'
    )
    
    transform=DbtTaskGroup(
        group_id='transform',
        project_config = DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method =LoadMode.DBT_LS,
            select=['path:models/intermediate']
        )
    )


    report=DbtTaskGroup(
        group_id='report',
        project_config = DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method =LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    chain(
        [*upload_csv_tasks, upload_exchange_rate_csv_to_gcs, *upload_parquet_tasks, *upload_json_tasks],  # Run all upload tasks in parallel
        create_dataset,
        [create_external_table, create_external_table_json, create_external_table_parquet, create_external_table_exchange_rate],  # Run these in parallel after dataset creation
        transform,
        report
    )


    
 


mobile_dag = mobile()  # Instantiate the DAG