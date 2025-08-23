from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging


def greet():
    logging.info("Hello from Airflow!")
    logging.warning("This is a warning message for testing purposes.")
    logging.error("This is an error message for testing purposes.")
    logging.critical("This is a critical message for testing purposes.")


def greet2():
    logging.info("Hello from Airflow 2!")
    logging.warning("This is a warning message for testing purposes.")
    logging.error("This is an error message for testing purposes.")
    logging.critical("This is a critical message for testing purposes.")


with DAG(
    dag_id="example_airflow_dag_ezz",
    schedule_interval=None,  # fixed here
    start_date=datetime(2025, 8, 23),
    catchup=False,
    tags=["example"],
) as dag:
    
    greet_task = PythonOperator(
        task_id="greet_task",
        python_callable=greet,
    )

    greet_task2 = PythonOperator(
        task_id="greet_task2",
        python_callable=greet2,
    )

    greet_task >> greet_task2
