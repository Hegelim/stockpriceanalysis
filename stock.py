from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import yfinance as yf
import pickle
import pandas as pd
from sklearn.linear_model import LinearRegression
from pathlib import Path


COMPANIES = ['AAPL', 'GOOGL', 'FB', 'MSFT', 'AMZN']
DF_DIRECTORY = "hw4/stock_dfs.pkl"
MODELS_DIRECTORY = "hw4/models.pkl"
FEATURE_COLS = ["Open", "Low", "Close", "Volume"]
PREDICTED_COL = ["High"]
RELATIVE_ERRORS_DIREC = "hw4/relative_errors.csv"

default_args = {
    'owner': 'yewen',
    'depends_on_past': False,
    'email': ['yz4175@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


def get_date_today():
    """Get date today in str format such as 20201119. """
    return datetime.today().strftime('%Y%m%d')


def get_new_data():
    """Read in models, read in csv, get new data, calculate relative errors,
    update csv.
    New data: pull all stock data up to and including the previous day.
    """
    relative_error_csv = Path(RELATIVE_ERRORS_DIREC)
    models_file = Path(MODELS_DIRECTORY)
    if models_file.is_file() and relative_error_csv.is_file():
        with open(MODELS_DIRECTORY, 'rb') as f:
            models = pickle.load(f)

        relative_errors = pd.read_csv(RELATIVE_ERRORS_DIREC, index_col=0)

    stock_dfs = []
    for i, company in enumerate(COMPANIES):
        # download all historic data
        historical = yf.download(company, )
        df = yf.download(company, period='10d')
        stock_dfs.append(df)
        if models_file.is_file() and relative_error_csv.is_file():
            model = models[i]
            # only today's
            X = df.tail(1)[FEATURE_COLS].to_numpy()
            # high_today = df.loc[get_date_today(), 'High']
            high_today = df.iloc[-1, :]['High']
            relative_error = (model.predict(X) - high_today) / high_today
            relative_errors[get_date_today()] = relative_error
            relative_errors.to_csv(RELATIVE_ERRORS_DIREC)
        else:
            data = {get_date_today(): ['First Day']}
            relative_error_df = pd.DataFrame.from_dict(data)
            relative_error_df.to_csv(RELATIVE_ERRORS_DIREC)

    with open(DF_DIRECTORY, 'wb') as f:
        pickle.dump(stock_dfs, f)


def make_linear_models():
    """Read in the data, make 5 Linear Regression models, save to file. """
    with open(DF_DIRECTORY, 'rb') as f:
        dfs = pickle.load(f)
    models = []
    for df in dfs:
        reg = LinearRegression()
        X = df[FEATURE_COLS].to_numpy()
        y = df[PREDICTED_COL].to_numpy()
        reg.fit(X, y)
        models.append(reg)
    with open(MODELS_DIRECTORY, 'wb') as f:
        pickle.dump(models, f)


with DAG('stock',
         default_args=default_args,
         description='Stock Price Analysis for HW4',
         catchup=False,
         start_date=datetime(2021, 1, 1),
         schedule_interval=timedelta(minutes=1)
         ) as dag:
    get_data_task = PythonOperator(task_id='get_data',
                                   python_callable=get_new_data)

    make_prediction_task = PythonOperator(task_id=f"receive_data",
                                          python_callable=make_linear_models)

    # task dependencies
    get_data_task >> make_prediction_task
