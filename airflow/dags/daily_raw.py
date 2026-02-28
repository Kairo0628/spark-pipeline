from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

import logging
import requests
import json
from datetime import datetime

FILE_NAME ={
    'SPOP_LOCAL_RESD_DONG': 'dong_foot_traffic',
    'CardBusStatisticsServiceNew': 'bus_stop_passenger',
    'tpssStationRouteTurn': 'bus_stop_trip_count',
    'tpssEmdBus': 'bus_dong_passenger'
}

def extract(api_id, target_date, **context):
    file_name = FILE_NAME[api_id]

    logging.info('Check Link')
    api_key = Variable.get('api_key')
    base_url = Variable.get('base_url')
    url_check = f'{base_url}/{api_key}/json/{api_id}/1/1/{target_date}'

    response = requests.get(url = url_check)
    check = response.json()
    try:
        end = check[api_id]['list_total_count']

        logging.info(f'{file_name} Extract Start')
        rows = []
        for i in range(1, end, 1000):
            url = f'{base_url}/{api_key}/json/{api_id}/{i}/{i + 999}/{target_date}'
            response = requests.get(url = url)
            row = response.json()[api_id]['row']
            rows += row
        logging.info(f'{file_name} Extract Complete')
    except Exception as E:
        logging.info(f'{file_name} Extract Error')
        raise E
    
    return rows

def extract_bus_stop_trip_count(api_id, target_date, **context):
    file_name = FILE_NAME[api_id]

    logging.info('Check Link')
    api_key = Variable.get('api_key')
    base_url = Variable.get('base_url')
    url_check = f'{base_url}/{api_key}/json/{api_id}/1/1/{target_date}'

    response = requests.get(url = url_check)
    check = response.json()
    try:
        end = check[api_id]['list_total_count']

        logging.info(f'{file_name} Extract Start')
        stop = False
        rows = []
        for i in range(1, end, 1000):
            url = f'{base_url}/{api_key}/json/{api_id}/{i}/{i + 999}/{target_date}'
            response = requests.get(url = url)
            row = response.json()[api_id]['row']
            if row[-1]['CRTR_DD'] != target_date:
                for j in row:
                    if j['CRTR_DD'] == target_date:
                        rows.append(j)
                    else:
                        stop = True
                        break
            else:
                rows += row
            
            if stop:
                break
            
        logging.info(f'{file_name} Extract Complete')
    except Exception as E:
        logging.info(f'{file_name} Extract Error')
        raise E
    
    return rows

def upload_gcs(api_id, prev_task, ds, ti, **context):
    file_name = FILE_NAME[api_id]
    logging.info(f'{file_name} Upload Start')

    rows = ti.xcom_pull(task_ids = prev_task)
    json_rows = json.dumps(rows)

    hook = GCSHook(gcp_conn_id = 'gcp_conn_id')
    hook.upload(
        bucket_name = 'spark-pipeline-bucket',
        object_name = f'raw_data/daily/dt={ds}/{file_name}.json',
        data = json_rows,
        encoding = 'utf-8'
    )

    logging.info(f'{file_name} Upload Complete')

with DAG(
    dag_id = 'daily_raw_data_dag',
    description = 'Extract Daily Raw Data and Load to GCS',
    start_date = datetime(2026, 2, 26),
    schedule = '30 0 * * *', # 매월 1일. UTC: 00:30, KST: 09:30
    tags = ['Monthly', 'Raw']
) as dag:
    
    dong_foot_traffic_extract = PythonOperator(
        task_id = 'dong_foot_traffic_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'SPOP_LOCAL_RESD_DONG',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    dong_foot_traffic_upload = PythonOperator(
        task_id = 'dong_foot_traffic_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'SPOP_LOCAL_RESD_DONG',
            'prev_task': 'dong_foot_traffic_extract'
        }
    )

    bus_stop_passenger_extract = PythonOperator(
        task_id = 'bus_stop_passenger_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'CardBusStatisticsServiceNew',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    bus_stop_passenger_upload = PythonOperator(
        task_id = 'bus_stop_passenger_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'CardBusStatisticsServiceNew',
            'prev_task': 'bus_stop_passenger_extract'
        }
    )

    bus_stop_trip_count_extract = PythonOperator(
        task_id = 'bus_stop_trip_count_extract',
        python_callable = extract_bus_stop_trip_count,
        op_kwargs = {
            'api_id': 'tpssStationRouteTurn',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    bus_stop_trip_count_upload = PythonOperator(
        task_id = 'bus_stop_trip_count_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'tpssStationRouteTurn',
            'prev_task': 'bus_stop_trip_count_extract'
        }
    )

    bus_dong_passenger_extract = PythonOperator(
        task_id = 'bus_dong_passenger_extract',
        python_callable = extract,
        op_kwargs = {
            'api_id': 'tpssEmdBus',
            'target_date': '{{ macros.ds_format(macros.ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    bus_dong_passenger_upload = PythonOperator(
        task_id = 'bus_dong_passenger_upload',
        python_callable = upload_gcs,
        op_kwargs = {
            'api_id': 'tpssEmdBus',
            'prev_task': 'bus_dong_passenger_extract'
        }
    )

    dong_foot_traffic_extract >> dong_foot_traffic_upload
    bus_stop_passenger_extract >> bus_stop_passenger_upload
    bus_stop_trip_count_extract >> bus_stop_trip_count_upload
    bus_dong_passenger_extract >> bus_dong_passenger_upload
