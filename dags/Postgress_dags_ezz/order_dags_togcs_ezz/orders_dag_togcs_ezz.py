from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime

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
# DAG Definition
# ----------------------------
with DAG(
    dag_id="postgres_to_gcs_incremental_operator_ezz", #dag id
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
        PostgresToGCSOperator(
            task_id=f"load_{table}_to_gcs",
            postgres_conn_id="postgres_orders_ezzeldein",   # connection ID  addedfrom Airflow UI
            sql=f"""
                SELECT * FROM public.{table}
                WHERE updated_at_timestamp >= '{{{{ ds }}}}'::date
            """,
            bucket=BUCKET_NAME,
            filename=f"{table}/{table}_{{{{ ds_nodash }}}}.csv",
            export_format="csv",
            gzip=False,
        )
