from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.models import Connection
from airflow.settings import Session

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
    schedule_interval="30 12 * * *",  # daily at 12:30 UTC = 3:30 PM Egypt
    start_date=datetime(2025, 8, 22),
    catchup=False,
    tags=["postgres", "gcs", "etl"],
) as dag:

    # Define Postgres connection dynamically (inside DAG)
    def register_postgres_conn():
        session = Session()
        conn_id = "postgres_orders_products"
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if not existing_conn:
            conn = Connection(
                conn_id=conn_id,
                conn_type="postgres",
                host="34.173.180.170",
                login="postgres",
                password="Ready-de26",
                port=5432,
                schema="postgres"
            )
            session.add(conn)
            session.commit()

    register_postgres_conn()

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
            postgres_conn_id="postgres_orders_products",  # registered above
            sql=f"""
                SELECT * 
                FROM public.{tbl}
                WHERE updated_at_timestamp >= '{{{{ ds }}}}'
            """,
            bucket=BUCKET_NAME,
            filename=f"{tbl}/dt={{{{ ds_nodash }}}}/{tbl}-{{{{ ds_nodash }}}}.csv",
            export_format="csv",
            gzip=False,
        )
