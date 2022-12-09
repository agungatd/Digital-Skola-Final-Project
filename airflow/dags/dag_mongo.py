from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "agung",
    "start_date": datetime(2022, 12, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    "mongo_etl",
    description= "api_kafka_etl_to_postgres",
    catchup=False,
    schedule_interval= None,
    default_args=default_args
) as dag:

    start = DummyOperator(task_id="Start", dag=dag)

    mongo_etl = BashOperator(
        task_id="mongo_etl",
        bash_command="python3 /opt/airflow/scripts/mongo_etl.py",
        dag=dag
    )

    stop = DummyOperator(task_id="Stop", dag=dag)

    start >> mongo_etl >> stop