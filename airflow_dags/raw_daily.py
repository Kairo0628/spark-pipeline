from airflow.sdk import DAG, Variable, task, TaskGroup
from airflow.sdk.execution_time.macros import ds_add, ds_format
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from datetime import datetime
import json
import logging
import os
import requests

FILE_NAME = {
    'CardBusStatisticsServiceNew': 'raw_route_stop_total_passenger',
    'tpssEmdBus': 'raw_dong_hour_passenger',
    'tpssStationRouteTurn': 'raw_route_stop_sequence'
}

@task
def fetch_total_rows(ds, api_id):
    base_url = Variable.get('base_url')
    api_key = Variable.get('api_key')
    target_date = ds_format(ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d")
    file_name = FILE_NAME[api_id]

    logging.info('Checking URL')

    try:
        url_check = f'{base_url}/{api_key}/json/{api_id}/1/1/{target_date}'

        response = requests.get(url = url_check)
        data = response.json()

        if 'RESULT' in data:
            if data['RESULT']['MESSAGE'] == '해당하는 데이터가 없습니다.':
                logging.warning(f'No data for target date. Target: {file_name}')
                raise AirflowSkipException


        total_rows = data[api_id]['list_total_count']

    except Exception as e:
        logging.error(f'URL Check Failed. Target: {file_name}')
        logging.error(e)
        raise e

    logging.info('URL Check Complete')

    return total_rows

@task
def ingest(ds, api_id, total_rows):
    base_url = Variable.get('base_url')
    api_key = Variable.get('api_key')
    target_date = ds_format(ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d")
    file_name = FILE_NAME[api_id]

    logging.info('Ingestion Start')

    try:
        rows = []
        for i in range(1, total_rows, 1000):
            url = f'{base_url}/{api_key}/json/{api_id}/{i}/{i + 999}/{target_date}'
            response = requests.get(url = url)
            row = response.json()[api_id]['row']
            rows += row

    except Exception as e:
        logging.error(f'Ingestion Failed. Target: {file_name}')
        logging.error(e)
        raise e

    with open(f'/opt/airflow/data/{target_date}_{file_name}.json', 'w', encoding = 'utf-8') as  f:
        json.dump(rows, f, ensure_ascii = False)

    logging.info('Ingestion Complete')

    return {'total_rows': total_rows,
            'len_rows': len(rows)}

@task
def ingest_route_stop_sequence(ds, api_id, total_rows):
    base_url = Variable.get('base_url')
    api_key = Variable.get('api_key')
    target_date = ds_format(ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d")
    file_name = FILE_NAME[api_id]

    logging.info('Ingestion Start')

    try:
        stop = False
        rows = []
        for i in range(1, total_rows, 1000):
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

    except Exception as e:
        logging.error(f'Ingestion Failed. Target: {file_name}')
        logging.error(e)
        raise e

    with open(f'/opt/airflow/data/{target_date}_{file_name}.json', 'w', encoding = 'utf-8') as  f:
        json.dump(rows, f, ensure_ascii = False)

    logging.info('Ingestion Complete')

    return {'total_rows': total_rows,
            'len_rows': len(rows)}

@task
def valid_rows(ingest_result):
    logging.info('Checking ingest result')

    total_rows = ingest_result['total_rows']
    len_rows = ingest_result['len_rows']

    logging.info(f'Total Rows: {total_rows}')
    logging.info(f'Ingest Rows: {len_rows}')

    if total_rows != len_rows:
        logging.error('Row count mismatch')
        raise AirflowFailException
    
    logging.info('Check Complete')

@task
def valid_file(ds, api_id):
    target_date = ds_format(ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d")
    file_name = FILE_NAME[api_id]

    logging.info('Checking file')

    with open(f'/opt/airflow/data/{target_date}_{file_name}.json', encoding = 'utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f'Invalid JSON. Target: {target_date}_{file_name}')
            raise e

    logging.info('JSON parse complete')

    if not isinstance(data, list):
        logging.error(f'Data is not a list. Target: {target_date}_{file_name}')
        raise AirflowFailException
    
    if not data:
        logging.error(f'File is Empty. Target: {target_date}_{file_name}')
        raise AirflowFailException
    
    logging.info('Check Complete')
    
def get_gcs_hook():
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    hook = GCSHook(gcp_conn_id = 'gcp_conn_id')

    return hook
    
@task
def upload_gcs(ds, api_id):
    target_date = ds_format(ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d")
    file_name = FILE_NAME[api_id]

    logging.info('Upload Start')

    hook = get_gcs_hook()
    hook.upload(
        bucket_name = 'my-spark-pipeline-bucket',
        object_name = f'raw_data/daily/dt={ds_add(ds, -5)}/{file_name}.json',
        filename = f'/opt/airflow/data/{target_date}_{file_name}.json',
        encoding = 'utf-8'
    )

    logging.info('Upload Complete')

@task
def valid_upload(ds, api_id):
    target_date = ds_format(ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d")
    file_name = FILE_NAME[api_id]

    logging.info('Checking GCS')

    hook = get_gcs_hook()

    gcs_file_size = hook.get_size(
        bucket_name = 'my-spark-pipeline-bucket',
        object_name = f'raw_data/daily/dt={ds_add(ds, -5)}/{file_name}.json',
    )
    local_file_size = os.path.getsize(f'/opt/airflow/data/{target_date}_{file_name}.json')

    if gcs_file_size != local_file_size:
        logging.error(f'File size mismatch')
        raise AirflowFailException

    logging.info('Check Complete')

@task
def cleanup(ds, api_id):
    target_date = ds_format(ds_add(ds, -5), "%Y-%m-%d", "%Y%m%d")
    file_name = FILE_NAME[api_id]

    logging.info('Cleanup file Start')

    os.remove(f'/opt/airflow/data/{target_date}_{file_name}.json')

with DAG(
    dag_id = 'raw_daily',
    description = 'Ingest Daily Raw and Load to GCS',
    start_date = datetime(2026, 11, 1),
    schedule = '30 5 * * *', # 매일. UTC: 05:30, KST: 14:30
    tags = ['Daily', 'Raw'],
    max_active_runs = 5
) as dag:
    
    for i, j in FILE_NAME.items():
        with TaskGroup(group_id = j):
            rows = fetch_total_rows(api_id = i)
            valid_list = [valid_file(api_id = i)]

            if i == 'tpssStationRouteTurn':
                ingest_result = ingest_route_stop_sequence(api_id = i, total_rows = rows)
            else:
                ingest_result = ingest(api_id = i, total_rows = rows)
                valid_list.append(valid_rows(ingest_result))

            ingest_result >> valid_list >> upload_gcs(api_id = i) >> valid_upload(api_id = i) >> cleanup(api_id = i)
