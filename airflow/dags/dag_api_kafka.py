from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "agung",
    "start_date": datetime(2022, 12, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    "api_kafka_etl",
    description= "api_kafka_etl_to_postgres",
    catchup=False,
    schedule_interval= None,
    default_args=default_args
) as dag:

    start = DummyOperator(task_id="Start", dag=dag)

    kafka_producer = SSHOperator(
        task_id="kafka_producer",
        ssh_conn_id="kafka_producer_ssh",
        command="python3 /usr/local/kafka/producer/main.py --worker 1 --bootstrap-server localhost:9092 --topic TopicCurrency",
        dag=dag
    )

    kafka_consumer = SSHOperator(
        task_id="kafka_consumer",
        ssh_conn_id = "kafka_consumer_ssh",
        command="python3 /usr/local/kafka/consumer/main.py --bootstrap-server localhost:9092 --topic TopicCurrency --tablename currencies",
        dag=dag
    )

    stop = DummyOperator(task_id="Stop", dag=dag)

    start >> kafka_producer >> kafka_consumer >> stop