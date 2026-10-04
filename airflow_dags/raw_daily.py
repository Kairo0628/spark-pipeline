from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

import logging
import requests
import json
import os
from datetime import datetime

FILE_NAME = {
    'CardBusStatisticsServiceNew': 'raw_route_stop_total_passenger',
    'tpssEmdBus': 'raw_dong_hour_passenger',
    'tpssStationRouteTurn': 'raw_route_stop_sequence'
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

def extract_route_stop_sequence(api_id, target_date, **context):
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

    stop = False
    rows = []
    for i in range(1, end, 1000):
        url = f'{base_url}/{api_key}/json/{api_id}/{i}/{i + 999}/{target_date}'
        response = requests.get(url = url)
        row = response.json()[api_id]['row']
        if row[-1]['CRTR_DD'] > target_date:
            continue
        elif row[-1]['CRTR_DD'] == target_date:
            if row[0]['CRTR_DD'] == target_date:
                rows += row
            else:
                for j in row:
                    if j['CRTR_DD'] == target_date:
                        rows.append(j)
        else:
            for j in row:
                if j['CRTR_DD'] == target_date:
                    rows.append(j)
                else:
                    stop = True
                    break
        
        if stop:
            break

    with open(f'/opt/airflow/data/{file_name}.json', 'w', encoding = 'utf-8') as f:
        json.dump(rows, f, ensure_ascii = False)

    logging.info(f'Extract Complete')

def upload_gcs(api_id, ds, **context):
    file_name = FILE_NAME[api_id]

    logging.info(f'{file_name} Upload Start')

    hook = GCSHook(gcp_conn_id = 'gcp_conn_id')
    hook.upload(
        bucket_name = 'my-spark-pipeline-bucket',
        object_name = f'raw_data/daily/dt={ds}/{file_name}.json',
        filename = f'/opt/airflow/data/{file_name}.json',
        encoding = 'utf-8'
    )
    logging.info(f'{file_name} Upload Complete')

    os.remove(f'/opt/airflow/data/{file_name}.json')
    logging.info('Json File Delete Complete')

with DAG(
    dag_id = 'raw_daily',
    description = 'Extract Daily Raw Data and Load to GCS',
    start_date = datetime(2026, 2, 28),
    schedule = '30 5 * * *', # 매일. UTC: 05:30, KST: 14:30
    tags = ['Daily', 'Raw'],
    max_active_runs = 1
) as dag:

    raw_route_stop_total_passenger_extract = PythonOperator(
        task_id = 'raw_route_stop_total_passenger_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'CardBusStatisticsServiceNew',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    raw_route_stop_total_passenger_upload = PythonOperator(
        task_id = 'raw_route_stop_total_passenger_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'CardBusStatisticsServiceNew'
        }
    )



    raw_dong_hour_passenger_extract = PythonOperator(
        task_id = 'raw_dong_hour_passenger_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'tpssEmdBus',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    raw_dong_hour_passenger_upload = PythonOperator(
        task_id = 'raw_dong_hour_passenger_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'tpssEmdBus'
        }
    )



    raw_route_stop_sequence_extract = PythonOperator(
        task_id = 'raw_route_stop_sequence_extract',
        python_callable = extract_route_stop_sequence,
        op_kwargs = {
            'api_id': 'tpssStationRouteTurn',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    raw_route_stop_sequence_upload = PythonOperator(
        task_id = 'raw_route_stop_sequence_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'tpssStationRouteTurn'
        }
    )

    raw_route_stop_total_passenger_extract >> raw_route_stop_total_passenger_upload
    raw_dong_hour_passenger_extract >> raw_dong_hour_passenger_upload
    raw_route_stop_sequence_extract >> raw_route_stop_sequence_upload
