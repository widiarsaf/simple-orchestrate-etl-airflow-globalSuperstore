from airflow.models.variable import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd
import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


DATASET_ID = Variable.get("DATASET_ID")
BASE_PATH = Variable.get("BASE_PATH")
BUCKET_NAME = Variable.get("BUCKET_NAME")
GOOGLE_CLOUD_CONN_ID = Variable.get("GOOGLE_CLOUD_CONN_ID")
BIGQUERY_TABLE_NAME = "bs_global_superstore"
GCS_OBJECT_NAME = "extract_global_superstore_data.csv"
DATA_PATH = f"{BASE_PATH}/data"
OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"



@dag(
    default_args={
        'owner': 'widia',
        'email': 'wretasafitri33@gmail.com',
        'email_on_failure': True
    },
    schedule_interval='0 4 * * * ',  # every 4AM
    start_date=days_ago(1),
    tags=['csv', 'tweet', 'disaster', 'blank-space']
)
def etl_global_superstore_dag():
    @task()
    def extract_transform():
      df = pd.read_csv(f"{DATA_PATH}/global_superstore.csv")

    #   Replace null in postal code column with 'None'
      df['Postal Code'].fillna('None', inplace=True)
    #   Change Order Date and Ship Date as Datetime
      df['Order Date'] = pd.to_datetime(df['Order Date'])
      df['Ship Date'] = pd.to_datetime(df['Ship Date'])
        
    #   Get order year, month, and day
      df['Order Year'] = df['Order Date'].dt.year
      df['Order Month'] = df['Order Date'].dt.strftime('%B')
      df['Order Day'] = df['Order Date'].dt.strftime('%A')

      df.to_csv(OUT_PATH, index=False, header=False)

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    extract_transform_task = extract_transform()

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        src=OUT_PATH,
        dst=GCS_OBJECT_NAME,
        bucket=BUCKET_NAME
    )

    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bigquery_conn_id=GOOGLE_CLOUD_CONN_ID,
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[  # based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'Row_ID', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'Order_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Order_Date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'Ship_Date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'Ship_Mode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Customer_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Customer_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Segment', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Postal_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Market', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Region', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Sub_Category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Sales', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Quantity', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'Discount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Profit', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Shipping_Cost', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Order_Priority', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Order_Year', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'Order_Month', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Order_Day', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        autodetect=False,
        # If the table already exists - overwrites the table data
        write_disposition='WRITE_TRUNCATE',
    )

    start >> extract_transform_task
    extract_transform_task >> stored_data_gcs
    stored_data_gcs >> loaded_data_bigquery
    loaded_data_bigquery >> end


global_superstore_etl= etl_global_superstore_dag()
