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
HISTORY_DIREC = 'hw4/histories.pkl'
RELATIVE_ERRORS_DIREC = "hw4/relative_errors.csv"
PREDICTION_DIREC = 'hw4/predictions.pkl'

FEATURE_COLS = ["Open", "Low", "Close", "Volume"]
PREDICTED_COL = ["High"]


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


def get_date_yesterday():
    """Return yesterday in str such as 2021-11-19. """
    return (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')


def get_data():
    """Get new data in the past 10 days up to including yesterday.
    For example, if today is 11/22/2021, only collect data up to
    including 11/21/2021. Append as a list. Save to pickle file.
    """
    stock_dfs = []
    for i, company in enumerate(COMPANIES):
        end = datetime.today() - timedelta(days=1)
        stock_df = yf.download(company, end=end)
        stock_df = stock_df.tail(10)
        stock_dfs.append(stock_df)
    with open(DF_DIRECTORY, 'wb') as f:
        pickle.dump(stock_dfs, f)


def get_historical_data():
    """Get all historical data up to including yesterday. """
    histories = []
    for i, company in enumerate(COMPANIES):
        history = yf.download(company, end=get_date_yesterday())
        histories.append(history)
    with open(HISTORY_DIREC, 'wb') as f:
        pickle.dump(histories, f)


def compute_relative_error():
    """Compute the relative error for today, update CSV.
    For example, today is 11/22, compute the relative error between
    prediction and high on 11/21.
    """
    with open(DF_DIRECTORY, 'rb') as f:
        # this data includes stock up to 11/21 in the past
        # 10 days
        stock_dfs = pickle.load(f)

    with open(PREDICTION_DIREC, 'rb') as f:
        # a list of 5 predictions made on 11/21.
        predictions = pickle.load(f)

    relative_errors_df = pd.read_csv(RELATIVE_ERRORS_DIREC)

    relative_errors = []
    for i, stock_df in enumerate(stock_dfs):
        # high price on 11/21.
        high = stock_df.iloc[-1, :]["High"]
        # prediction on 11/21.
        prediction = predictions[i]
        relative_error = prediction - high / high
        relative_errors.append(relative_error)

    relative_errors_df[get_date_yesterday()] = relative_errors
    relative_errors_df.to_csv(RELATIVE_ERRORS_DIREC)


def make_prediction():
    """Make a prediction for today. """
    with open(MODELS_DIRECTORY, 'rb') as f:
        models = pickle.load(f)

    with open(DF_DIRECTORY, 'rb') as f:
        stock_dfs = pickle.load(f)

    predictions = []
    for i, stock_df in enumerate(stock_dfs):
        model = models[i]
        X = stock_df[FEATURE_COLS].to_numpy().reshape((-1, 1))
        prediction = model.predict(X)[0]
        predictions.append(prediction)

    with open(PREDICTION_DIREC, 'wb') as f:
        pickle.dump(predictions, f)


def train_models():
    """Make Linear Regression Models. """
    with open(HISTORY_DIREC, 'rb') as f:
        histories_df = pickle.load(f)

    models = []
    for history_df in histories_df:
        reg = LinearRegression()
        X = 0

    return 0


with DAG('stock',
         default_args=default_args,
         description='Stock Price Analysis for HW4',
         catchup=False,
         start_date=datetime(2021, 1, 1),
         schedule_interval=timedelta(minutes=1)
         ) as dag:
    get_data_task = PythonOperator(task_id='get_data',
                                   python_callable=get_data)
