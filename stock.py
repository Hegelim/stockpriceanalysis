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

dag = DAG('Stock Price Analysis',
          default_args=default_args,
          description='Stock Price Analysis for HW4',
          catchup=False,
          start_date=datetime(2021, 1, 1),
          schedule_interval='* 7 * * *')

tickers = ['AAPL', 'GOOGL', 'FB', 'MSFT', 'AMZN']


def fetch_prices_function(**kwargs):
    """Downloads the full price history of stocks.

    Returns:
        A list of stock prices.
    """
    print('1 Fetching stock prices and remove duplicates...')
    stocks_prices = []
    for i in range(len(tickers)):
        prices = yf.download(tickers[i], period='max').iloc[:, :5].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()
        prices.insert(loc=1, column='Stock', value=tickers[i])
        stocks_prices.append(prices)
    return stocks_prices


def stocks_plot_function(**kwargs):
    """Given a list of stock prices, plot stock trends over time.

    Returns:
        None.
    """
    print('2 Pulling stocks_prices to concatenate sub-lists to create a combined dataset + write to CSV file...')
    ti = kwargs['ti']
    stocks_prices = ti.xcom_pull(task_ids='fetch_prices_task')
    stock_plots_data = pd.concat(stocks_prices, ignore_index=True)
    stock_plots_data.to_csv('/Users/anbento/Documents/Data_Sets/Medium/stocks_plots_data.csv', index=False)

    print('DF Shape: ', stock_plots_data.shape)
    print(stock_plots_data.head(5))


fetch_prices_task = PythonOperator(task_id='fetch_prices_task',
                                   python_callable=fetch_prices_function,
                                   provide_context=True,
                                   dag=dag)

stocks_plot_task = PythonOperator(task_id='stocks_plot_task',
                                  python_callable=stocks_plot_function,
                                  provide_context=True,
                                  dag=dag)

stocks_table_task = PythonOperator(task_id='stocks_table_task',
                                   python_callable=stocks_table_function,
                                   provide_context=True,
                                   dag=dag)

##########################################
# DEFINE TASKS HIERARCHY
##########################################

fetch_prices_task >> stocks_plot_task >> stocks_table_task
