from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# ---------------- Default Args ---------------- #
default_args = {
    "owner": "ezzeldein",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------- DAG ---------------- #
with DAG(
    dag_id="postgres_to_gcs_orders_products",
    default_args=default_args,
    description="Export Postgres tables into GCS as CSV",
    schedule_interval="30 12 * * *",  # runs daily at 3:30 PM Egypt (12:30 UTC)
    start_date=datetime(2025, 8, 30),
    catchup=False,
    tags=["postgres", "gcs", "etl"],
) as dag:

    TABLES = [
        "order_items",
        "order_reviews",
        "orders",
        "products",
        "product_category_name_translation",
    ]

    BUCKET_NAME = "orders_products_ezzeldein"

    for tbl in TABLES:
        PostgresToGCSOperator(
            task_id=f"postgres_to_gcs_{tbl}",
            postgres_conn_id="DB1",  # Your Airflow Postgres connection
            sql=f"SELECT * FROM public.{tbl};",
            bucket=BUCKET_NAME,
            filename=f"{tbl}-{{{{ ds_nodash }}}}.csv",  # Auto: table-YYYYMMDD.csv
            export_format="csv",
            gzip=False,
        )
