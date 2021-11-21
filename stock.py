from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import datetime as dt
import pandas as pd
import yfinance as yf
import requests
import lxml
from functools import reduce
import pickle


TICKERS = ['AAPL', 'GOOGL', 'FB', 'MSFT', 'AMZN']

############################################
# DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################
default_args = {
    'owner': 'yewen',
    'depends_on_past': False,
    'email': ['yz4175@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


def get_data_func():
    stock_dfs = []
    for task in TICKERS:
        df = yf.download(task, period='10d')
        stock_dfs.append(df)
    with open('stock_dfs.pkl', 'wb') as f:
        pickle.dump(stock_dfs, f)


def receive_data_func():
    with open('stock_dfs.pkl', 'rb') as f:
        dfs = pickle.load(f)
    for df in dfs:
        print(df.head())
        print(f"Type: {type(df)}")


with DAG('stock',
         default_args=default_args,
         description='Stock Price Analysis for HW4',
         catchup=False,
         start_date=datetime(2021, 1, 1),
         schedule_interval=timedelta(minutes=1)
         ) as dag:

    get_data_task = PythonOperator(task_id='get_data',
                                   python_callable=get_data_func)

    receive_data_task = PythonOperator(task_id=f"receive_data",
                                       python_callable=receive_data_func)

    # task dependencies
    get_data_task >> receive_data_task
