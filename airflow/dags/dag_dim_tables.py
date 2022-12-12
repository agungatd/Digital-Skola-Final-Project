from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "agung",
    "start_date": datetime(2022, 12, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    "create_dim_tables",
    description= "create all dim tables for dwh",
    catchup=False,
    schedule_interval= None,
    default_args=default_args
) as dag:

    start = DummyOperator(task_id="Start", dag=dag)

    # Dependency Mongo ETL DAG
    mongo_etl = BashOperator(
        task_id="mongo_etl",
        bash_command="python3 /opt/airflow/scripts/mongo_etl.py",
        dag=dag
    )

    # Dependency Mysql ETL DAG
    csv_to_mysql = SparkSubmitOperator(
        task_id = 'csv_to_mysql',
        application = '/usr/local/spark/app/csv_to_mysql.py',
        name='spark_mysql_etl1',
        conn_id='spark_default',
        jars='/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar',
        dag=dag
    )
    mysql_to_postgres = SparkSubmitOperator(
        task_id = 'mysql_to_postgres',
        application = '/usr/local/spark/app/mysql_to_postgres.py',
        name='spark_mysql_etl2',
        conn_id='spark_default',
        jars='/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar, /opt/bitnami/spark/jars/postgresql-42.2.25.jar',
        dag=dag
    )

    # Dim Tables
    dim_country_state = BashOperator(
        task_id="dim_country_state",
        bash_command="python3 /opt/airflow/scripts/dim_state_n_country.py",
        dag=dag
    )
    dim_city = BashOperator(
        task_id="dim_city",
        bash_command="python3 /opt/airflow/scripts/dim_city.py",
        dag=dag
    )
    dim_currency = BashOperator(
        task_id="dim_currency",
        bash_command="python3 /opt/airflow/scripts/dim_currency.py",
        dag=dag
    )

    stop = DummyOperator(task_id="Stop", dag=dag)

    # Orchestration
    related_dim_tables = (start >> mongo_etl >> dim_country_state >> dim_city)
    (
        start 
        >> [
            related_dim_tables, 
            dim_currency
        ] 
        >> stop
    )