from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

import logging
import requests
import json
import os
from datetime import datetime

FILE_NAME = {
    'masterRouteNode': 'bus_route_stop',
    'tbisMasterStation': 'bus_stop',
    'tbisMasterRoute': 'bus_route'
}

def extract(api_id):
    file_name = FILE_NAME[api_id]

    logging.info('Check Link')
    api_key = Variable.get('api_key')
    base_url = Variable.get('base_url')
    url_check = f'{base_url}/{api_key}/json/{api_id}/1/1'

    response = requests.get(url = url_check)
    check = response.json()
    try:
        end = check[api_id]['list_total_count']

        logging.info(f'{file_name} Extract Start')
        rows = []
        for i in range(1, end, 1000):
            url = f'{base_url}/{api_key}/json/{api_id}/{i}/{i + 999}'
            response = requests.get(url = url)
            row = response.json()[api_id]['row']
            rows += row

        with open(f'/opt/airflow/data/{file_name}.json', 'w', encoding = 'utf-8') as f:
            json.dump(rows, f, ensure_ascii = False)
        
        logging.info(f'{file_name} Extract Complete')
    except Exception as E:
        logging.info(f'{file_name} Extract Error')
        raise E

def upload_gcs(api_id, ds, **context):
    file_name = FILE_NAME[api_id]
    logging.info(f'{file_name} Upload Start')

    hook = GCSHook(gcp_conn_id = 'gcp_conn_id')
    hook.upload(
        bucket_name = 'spark-pipeline-bucket',
        object_name = f'raw_data/monthly/dt={ds}/{file_name}.json',
        filename = f'/opt/airflow/data/{file_name}.json',
        encoding = 'utf-8'
    )
    logging.info(f'{file_name} Upload Complete')

    os.remove(f'/opt/airflow/data/{file_name}.json')
    logging.info('Json File Delete Complete')

with DAG(
    dag_id = 'monthly_raw_data_dag_v2',
    description = 'Extract Monthly Raw Data and Load to GCS',
    start_date = datetime(2026, 2, 28),
    schedule = '30 5 1 * *', # 매월 1일. UTC: 05:30, KST: 14:30
    tags = ['Monthly', 'Raw'],
    max_active_runs = 1
) as dag:
    
    bus_route_stop_extract = PythonOperator(
        task_id = 'bus_route_stop_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'masterRouteNode'
        }
    )

    bus_route_stop_upload = PythonOperator(
        task_id = 'bus_route_stop_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'masterRouteNode',
        }
    )

    bus_stop_extract = PythonOperator(
        task_id = 'bus_stop_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'tbisMasterStation'
        }
    )

    bus_stop_upload = PythonOperator(
        task_id = 'bus_stop_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'tbisMasterStation',
        }
    )

    bus_route_extract = PythonOperator(
        task_id = 'bus_route_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'tbisMasterRoute'
        }
    )

    bus_route_upload = PythonOperator(
        task_id = 'bus_route_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'tbisMasterRoute',
        }
    )

    bus_route_stop_extract >> bus_route_stop_upload
    bus_stop_extract >> bus_stop_upload
    bus_route_extract >> bus_route_upload
