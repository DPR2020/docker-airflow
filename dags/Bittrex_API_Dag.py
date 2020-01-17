# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 23:46:27 2020

@author: SatishChetty
"""

# -*- coding: utf-8 -*-
"""
Created on Tue May 28 16:34:52 2019

@author: RayBao
"""
import base64
import requests
import pandas as pd
import json
import csv
import io

from azure.storage.blob import BlockBlobService


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'SCHETTY123',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 30, 0, 0, 0),
    'email': ['schetty@protonmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('Update_Bittrex_API_Data', catchup=False,
          default_args=default_args, schedule_interval='0 */4 * * *')

accountName = "testcorpb"
accountKey = "YoOee2QfSQDPy1KftDVOkrlfQQttjD7jdcr4CieDlsIAMx89R/zCBIm/dDT60EPPxV3FR5fKxXcD7Uc0WJ9qGQ=="
containerName = "check"
blobService = BlockBlobService(accountName, accountKey)

def update_snowflake_v3_markets():
    df = pd.read_json(json.dumps(requests.get('https://api.bittrex.com/v3/markets').json()))
    output = io.StringIO()
    output = df.to_csv(index=False, encoding="utf-8", quoting=csv.QUOTE_NONNUMERIC)
    filepath = 'bittrex_api/v3/markets.csv'
    print("Writing to Azure blob: " + filepath)
    blobService.create_blob_from_text(containerName, filepath, output)
    # return df

def update_snowflake_v1_markets():
    df = pd.read_json(json.dumps(requests.get(
        'https://api.bittrex.com/api/v1.1/public/getmarkets').json()['result']))
    output = io.StringIO()
    output = df.to_csv(index=False, encoding="utf")
    filepath = 'bittrex_api/v1/markets.csv'
    print("Writing to Azure blob: " + filepath)
    blobService.create_blob_from_text(containerName, filepath, output)

update_v3_markets = PythonOperator(
    task_id='update_snowflake_v3_markets',
    provide_context=False,
    python_callable=update_snowflake_v3_markets,
    dag=dag
)

update_v1_markets = PythonOperator(
    task_id='update_snowflake_v1_markets',
    provide_context=False,
    python_callable=update_snowflake_v1_markets,
    dag=dag
)

update_v3_markets >> update_v1_markets
