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
    "create_fct_table_monthly",
    description= "create all fact tables for dwh monthly",
    schedule_interval='@monthly',
    catchup=False,
    default_args=default_args
) as dag:

    start = DummyOperator(task_id="Start", dag=dag)

    # Fact Tables
    fct_currency_monthly_avg = BashOperator(
        task_id="dim_country_state",
        bash_command="python3 /opt/airflow/scripts/fct_currency_monthly_avg.py",
        dag=dag
    )

    stop = DummyOperator(task_id="Stop", dag=dag)

    # Orchestration
    (
        start 
        >> fct_currency_monthly_avg
        >> stop
    )