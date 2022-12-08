from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'agung',
    'start_date': datetime(2022, 12, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    'spark_mysql_etl',
    description= 'spark_mysql_etl',
    catchup=False,
    schedule_interval= None,
    default_args=default_args
) as dag:

    start = DummyOperator(task_id='Start', dag=dag)

    mysql_etl = SparkSubmitOperator(
        task_id = 'spark-job',
        application = '/usr/local/spark/app/csv_to_mysql.py',
        name='spark_mysql_etl',
        conn_id='spark_default',
        jars='/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar',
        driver_class_path='/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar',
        dag=dag
    )

    # mysql_etl = BashOperator(
    #     task_id = 'spark-mysql-etl',
    #     bash_command='python3 /usr/local/spark/app/csv_to_mysql.py',
    #     dag=dag
    # )

    stop = DummyOperator(task_id='Stop', dag=dag)

    start >> mysql_etl >> stop