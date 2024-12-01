import os
import datetime
import pandas as pd
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.decorators import task

INPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'input')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'output')
ELASTIC_SEARCH_URL = "http://elasticsearch-kibana:9200"

with DAG(
        dag_id="first_lab",
        schedule="0 0 * * *",
        schedule_interval=None,
        dagrun_timeout=datetime.timedelta(minutes=60),
        start_date=datetime.datetime(2024, 11, 28, tzinfo=datetime.timezone.utc), 
        catchup=False,
) as dag:

    @task(task_id='read_file')
    def read_file():
        df = pd.concat([pd.read_csv(os.path.join(INPUT_DIR, f'chunk{i}.csv')) for i in range(26)], axis=0)
        df.to_csv('result.csv', index=False)

    @task(task_id='dropna')
    def dropna():
        df = pd.read_csv('result.csv')
        df.dropna(subset=['designation', 'region_1'], inplace=True, axis=0)
        df.to_csv('result.csv', index=False)

    @task(task_id='fillna')
    def fillna():
        df = pd.read_csv('result.csv')
        df.fillna({'price': 0.0})
        df.to_csv('result.csv', index=False)


    @task(task_id='save_csv')
    def save_csv():
        df = pd.read_csv('result.csv')
        df.to_csv(os.path.join(OUTPUT_DIR, 'output.csv'), index=False)

    @task(task_id='save_els')
    def save_els():
        df = pd.read_csv('result.csv')
        elastic_search = Elasticsearch(ELASTIC_SEARCH_URL)
        for _, row in df.iterrows():
            elastic_search.index(index='first_lab_final_data', body=row.to_json())

    first_task, second_task, third_task, four_task, final_task = read_file(), dropna(), fillna(), save_csv(), save_els()

    first_task >> second_task >> third_task >> [four_task, final_task]