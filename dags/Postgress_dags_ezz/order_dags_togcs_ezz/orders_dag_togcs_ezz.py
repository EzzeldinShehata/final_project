from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import storage
from datetime import datetime
import pandas as pd
import os

# ----------------------------
# Default DAG arguments
# ----------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 22),  # first run reference date
    "retries": 1,
}

# ----------------------------
# GCS bucket
# ----------------------------
BUCKET_NAME = "orders_products_ezzeldein"

# ----------------------------
# Extract + Upload function
# ----------------------------
def extract_and_upload(table, execution_date, **kwargs):
    # Connect to Postgres without needing Admin setup
    pg_hook = PostgresHook(
        postgres_conn_id=None,
        host="34.173.180.170",
        schema="postgres",
        login="postgres",
        password="Ready-de26",
        port=5432,
    )

    sql = f"""
        SELECT *
        FROM public.{table}
        WHERE updated_at_timestamp <= '{execution_date}'::date
    """
    df = pg_hook.get_pandas_df(sql)

    if df.empty:
        print(f"No new rows for table {table} on {execution_date}")
        return

    # Save as CSV locally
    file_name = f"/tmp/{table}-{execution_date.replace('-', '')}.csv"
    df.to_csv(file_name, index=False)

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{table}/{os.path.basename(file_name)}")
    blob.upload_from_filename(file_name)

    print(f"Uploaded {file_name} to gs://{BUCKET_NAME}/{table}/")

# ----------------------------
# DAG Definition
# ----------------------------
with DAG(
    dag_id="postgres_to_gcs_incremental_ezz",
    default_args=default_args,
    schedule_interval="30 15 * * *",  # run daily at 3:30 PM UTC
    catchup=False,
    tags=["postgres", "gcs", "etl"],
) as dag:

    tables = [
        "order_items",
        "order_reviews",
        "orders",
        "products",
        "product_category_name_translation",
    ]

    for table in tables:
        PythonOperator(
            task_id=f"load_{table}_to_gcs",
            python_callable=extract_and_upload,
            op_kwargs={
                "table": table,
                "execution_date": "{{ ds }}",  # Airflow will inject the run date
            },
            provide_context=True,
        )
