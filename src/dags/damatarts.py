from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_datamarts_calculation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False 
) as dag:


    create_datamarts_dir = BashOperator(
        task_id='create_datamarts_directory',
        bash_command=(
            'hdfs dfs -mkdir -p '
            'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/careenin/datamarts'
        )
    )

    process_events = BashOperator(
        task_id='process_events',
        bash_command='spark-submit /path/to/process_events.py'
    )


    process_city_aggregates = BashOperator(
        task_id='process_city_aggregates',
        bash_command='spark-submit /path/to/process_city_aggregates.py'
    )


    friend_recommendation_mart = BashOperator(
        task_id='friend_recommendation_mart',
        bash_command='spark-submit /path/to/friend_recommendation_mart.py'
    )

    create_datamarts_dir >> process_events >> process_city_aggregates >> friend_recommendation_mart
