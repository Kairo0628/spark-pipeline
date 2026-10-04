from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

import logging
import requests
import json
import os
from datetime import datetime

FILE_NAME = {
    'tbisMasterStation': 'raw_stop_master',
    'busRoute': 'raw_route_id',
    'CardBusTimeNew': 'raw_route_stop_hour_passenger'
}

def extract(api_id, target_date, **context):
    file_name = FILE_NAME[api_id]
    api_key = Variable.get('api_key')
    base_url = Variable.get('base_url')

    logging.info('URL Check')

    try:
        url_check = f'{base_url}/{api_key}/json/{api_id}/1/1/{target_date}'

        response = requests.get(url = url_check)
        end = response.json()[api_id]['list_total_count']

        logging.info('URL Check Complete')
    except Exception as e:
        logging.info(f'URL Check Error')
        logging.info(e)
        raise e
    
    logging.info(f'Extract Start')

    rows = []
    for i in range(1, end, 1000):
        url = f'{base_url}/{api_key}/json/{api_id}/{i}/{i + 999}/{target_date}'
        response = requests.get(url = url)
        row = response.json()[api_id]['row']
        rows += row

    with open(f'/opt/airflow/data/{file_name}.json', 'w', encoding = 'utf-8') as f:
        json.dump(rows, f, ensure_ascii = False)
    
    logging.info(f'Extract Complete')

def upload_gcs(api_id, ds, **context):
    file_name = FILE_NAME[api_id]

    logging.info(f'{file_name} Upload Start')

    hook = GCSHook(gcp_conn_id = 'gcp_conn_id')
    hook.upload(
        bucket_name = 'my-spark-pipeline-bucket',
        object_name = f'raw_data/monthly/dt={ds}/{file_name}.json',
        filename = f'/opt/airflow/data/{file_name}.json',
        encoding = 'utf-8'
    )
    logging.info(f'{file_name} Upload Complete')

    os.remove(f'/opt/airflow/data/{file_name}.json')
    logging.info('Json File Delete Complete')

with DAG(
    dag_id = 'raw_monthly',
    description = 'Extract Monthly Raw Data and Load to GCS',
    start_date = datetime(2026, 2, 28),
    schedule = '40 5 5 * *', # 매월 5일. UTC: 05:40, KST: 14:40
    tags = ['Monthly', 'Raw'],
    max_active_runs = 1
) as dag:
    
    raw_stop_master_extract = PythonOperator(
        task_id = 'raw_stop_master_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'tbisMasterStation',
            'target_date': '{{ ds_nodash }}'
        }
    )

    raw_stop_master_upload = PythonOperator(
        task_id = 'raw_stop_master_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'tbisMasterStation',
        }
    )



    raw_route_id_extract = PythonOperator(
        task_id = 'raw_route_id_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'busRoute',
            'target_date': '{{ ds_nodash }}'
        }
    )

    raw_route_id_upload = PythonOperator(
        task_id = 'raw_route_id_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'busRoute',
        }
    )



    raw_route_stop_hour_passenger_extract = PythonOperator(
        task_id = 'raw_route_stop_hour_passenger_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'CardBusTimeNew',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -6), "%Y-%m-%d", "%Y%m") }}'
        }
    )

    raw_route_stop_hour_passenger_upload = PythonOperator(
        task_id = 'raw_route_stop_hour_passenger_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'CardBusTimeNew',
        }
    )

    raw_stop_master_extract >> raw_stop_master_upload
    raw_route_id_extract >> raw_route_id_upload
    raw_route_stop_hour_passenger_extract >> raw_route_stop_hour_passenger_upload
