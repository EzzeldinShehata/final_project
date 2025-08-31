from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    dag_id="postgres_to_gcs_orders_products_ezz",
    default_args=default_args,
    description="Incremental export of Postgres tables into GCS",
    schedule_interval="30 12 * * *",  # daily at 3:30 PM Egypt (12:30 UTC)
    start_date=datetime(2025, 8, 22),
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

    # Build connection dynamically
    POSTGRES_CONN_URI = "postgresql://postgres:Ready-de26@34.173.180.170:5432/postgres"

    for tbl in TABLES:
        PostgresToGCSOperator(
            task_id=f"postgres_to_gcs_{tbl}",
            postgres_conn_id=None,  # we override with hook below
            sql=f"""
                SELECT * 
                FROM public.{tbl}
                WHERE updated_at_timestamp >= '{{{{ ds }}}}'
            """,
            bucket=BUCKET_NAME,
            filename=f"{tbl}/dt={{{{ ds_nodash }}}}/{tbl}-{{{{ ds_nodash }}}}.csv",
            export_format="csv",
            gzip=False,
            # Here we inject the connection URI dynamically
            postgres_hook=PostgresHook(postgres_conn_id=None, uri=POSTGRES_CONN_URI),
        )
