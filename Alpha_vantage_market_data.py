import requests
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import time
import json
import pandas as pd
import numpy as np
import os

default_dag_args = {
    'start_date' : datetime(2023, 2, 4) ,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay': timedelta(minutes = 5),
    'project_id :': 1
}

#Get_data expecting a dictionary
def get_data(**kwargs):
    ticker = kwargs['ticker']
    Api_key = 'RUQQGJZAW7XAL583'
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo' + Api_key
    r = requests.get(url)
    try:
        data = r.json()
        path = '/opt/airflow/data/DATA_CENTER/DATA_LAKE'
        with open(path + "stock_market_raw_data" + ticker + "_" + str(time.time()), 'w') as outfile:
            json.dump(data, outfile)
    except:
        pass

def test_data_first_option(**kwargs):
    read_path = '/opt/airflow/data/DATA_CENTER/'
    ticker = kwargs['ticker']
    latest = np.min([float(file.split("_")[-1][7::]) for file in os.listdir(read_path) if (ticker in file) and ('.csv' not in file)])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]

    file = open(read_path + latest_file)
    data = json.load(file)

    condition_1 = len(data.keys()) == 2
    condition_2 = 'Time Series (5min)' in data.keys()
    condition_3 = 'Meta Data' in data.keys()

    if condition_1 and condition_2 and condition_3:
        pass
    else:
        raise Exception("The data integrity has been compromised")

def test_data_second_option(**kwargs):
    read_path = '/opt/airflow/data/DATA_CENTER/'
    ticker = kwargs['ticker']
    latest = np.min([float(file.split("_")[-1][7::]) for file in os.listdir(read_path) if (ticker in file) and ('.csv' not in file)])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]

    file = open(read_path + latest_file)
    data = json.load(file)

    condition_1 = len(data.keys()) == 2
    condition_2 = 'Time Series (5min)' in data.keys()
    condition_3 = 'Meta Data' in data.keys()

    if condition_1 and condition_2 and condition_3:
        #For the branch operator we'd like to return the name of another task
        return "clean_market_data"
    else:
        return "failed_task_data"
          

def clean_data(**kwargs):
    output_path = '/opt/airflow/data/DATA_CENTER/CLEAN_DATA'
    read_path = '/opt/airflow/data/DATA_CENTER/'
    ticker = kwargs['ticker']
    latest = np.min([float(file.split("_")[-1][7::]) for file in os.listdir(read_path) if (ticker in file) and ('.csv' not in file)])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]
    
    
    file = open(read_path + latest_file)
    data = json.load(file) 
    clean_data = pd.DataFrame(data['Time Series (5min)'])
    clean_data = clean_data.T
    clean_data['ticker'] = data['Meta Data']['2. Symbol']
    clean_data['meta_data'] = str(data['Meta Data'])
    clean_data['timestamp'] = pd.to_datetime('now')
    
    #We will want to store the result in the clean data lake
    clean_data.to_csv(output_path + ticker + "_snapshot_daily" + str(pd.to_datetime('now')) + '.csv')

with DAG("market_data_alphavantage_dag", schedule_interval = '@daily', catchup = False, default_args = default_dag_args ) as market_python:
    #here we define our tasks
    task_0 = PythonOperator(task_id = 'get_market_data', python_callable = get_data, op_kwargs = {'ticker' : "IBM" })
    task_1 = BranchPythonOperator(task_id = 'test_market_data', python_callable = test_data_second_option, op_kwargs = {'ticker' : "IBM" })  
    task_2_1 = PythonOperator(task_id = 'clean_market_data', python_callable = clean_data, op_kwargs = {'ticker' : "IBM" })   
    task_2_2 = DummyOperator(task_id = 'failed_task_data')
    task_3 = DummyOperator(task_id = 'aggregate_all_data_to_db')

    task_0 >> task_1
    task_1 >> task_2_1 >> task_3
    task_1 >> task_2_2 >> task_3
