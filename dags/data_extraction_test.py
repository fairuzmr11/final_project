from datetime import datetime, timedelta
import requests
import subprocess
import json 
import pandas as pd
import numpy as np
# from partialjson.json_parser import JSONParser

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def api_url_request():
    api_url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab%22"

    try:
        response = requests.get(api_url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON data from the response
            data = response.json()

            # Now 'data' contains the information retrieved from the API
            # You can manipulate or analyze the data as needed

            print(data)
            raw_content = data['data']['content']
            df = pd.DataFrame(raw_content)
            return True
        else:
            print(f"Error: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

# DAG definition
with DAG(
    dag_id='data_extraction_test',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=10)
    }
) as dag:
    
    start = EmptyOperator(task_id='start')

    api_url_request = PythonOperator(
        task_id='api_url_request',
        python_callable=api_url_request,
        execution_timeout=timedelta(minutes=5)
    )

    # json_cleaning_to_df = PythonOperator(
    #     task_id = 'json_to_df',
    #     python_callable= json_cleaning_to_df,
    #     execution_timeout=timedelta(minutes=5)
    # )

    end = EmptyOperator(task_id='end')

    start >> api_url_request >> end
