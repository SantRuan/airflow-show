from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import requests 


def print_random_quote():
    response = requests.get('http://api.quotable.io/random')
    return f'{response.json()["content"]}'  # Corrigi as aspas aqui também

def print_welcome():
    return "welcome Ruan"

with DAG(
    "my_first_dag",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule=timedelta(seconds=30),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"]
) as dag:
    t1 = PythonOperator(
        task_id="print_welcome",
        python_callable=print_welcome,
        dag=dag
    )
    t2 = PythonOperator(
        task_id="print_random_quote",
        python_callable=print_random_quote,
        dag=dag
    )

t1 >> t2