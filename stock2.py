from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import yfinance as yf
import pickle
import pandas as pd
from sklearn.linear_model import LinearRegression
from pathlib import Path
import logging


COMPANIES = ['AAPL', 'GOOGL', 'FB', 'MSFT', 'AMZN']

DF_DIRECTORY = "hw4/stock_dfs.pkl"
MODELS_DIRECTORY = "hw4/models.pkl"
HISTORY_DIREC = 'hw4/histories.pkl'
RELATIVE_ERRORS_DIREC = "hw4/relative_errors.csv"
PREDICTION_DIREC = 'hw4/predictions.pkl'

FEATURE_COLS = ["Open", "Low", "Close", "Volume", 'High']
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
        # because I run at 7 am everyday, using period='10d'
        # would get the data in the past 10 days including
        # yesterday
        stock_df = yf.download(company, period='10d')
        stock_dfs.append(stock_df)
    with open(DF_DIRECTORY, 'wb') as f:
        pickle.dump(stock_dfs, f)


def get_historical_data():
    """Get all historical data up to including yesterday. """
    histories = []
    for i, company in enumerate(COMPANIES):
        # because I run at 7 am every day, this would
        # actually just get me all the historic data
        history = yf.download(company, period='max')
        histories.append(history)
    with open(HISTORY_DIREC, 'wb') as f:
        pickle.dump(histories, f)


def compute_relative_error():
    """Compute the relative error for today, update CSV.
    For example, today is 11/22, compute the relative error between
    prediction and high on 11/21.

    On the 1st run, it will pass.
    On the 2nd run, it will create a csv.
    On the later runs, it will read the csv.
    """
    with open(DF_DIRECTORY, 'rb') as f:
        # this data includes stock up to 11/21 in the past
        # 10 days
        stock_dfs = pickle.load(f)

    if Path(PREDICTION_DIREC).is_file():
        with open(PREDICTION_DIREC, 'rb') as f:
            # a list of 5 predictions made on 11/21.
            predictions = pickle.load(f)

    if Path(PREDICTION_DIREC).is_file():
        relative_errors = []
        for i, stock_df in enumerate(stock_dfs):
            # high price on 11/21.
            high = stock_df.iloc[-1, :]["High"]
            # prediction on 11/21.
            prediction = predictions[i]
            relative_error = (prediction - high) / high
            relative_errors.append(relative_error)

        if Path(RELATIVE_ERRORS_DIREC).is_file():
            logging.info("Updating relative_errors csv...")
            relative_errors_df = pd.read_csv(RELATIVE_ERRORS_DIREC)
            relative_errors_df[get_date_yesterday()] = relative_errors
            relative_errors_df.to_csv(RELATIVE_ERRORS_DIREC)
        else:
            # create a CSV
            logging.info("Creating relative_errors csv...")
            relative_errors_df = pd.DataFrame(data=relative_errors,
                                              columns=[get_date_yesterday()],
                                              index=COMPANIES)
            relative_errors_df.to_csv(RELATIVE_ERRORS_DIREC)


def train_models():
    """Make Linear Regression Models. """
    with open(HISTORY_DIREC, 'rb') as f:
        histories_df = pickle.load(f)

    models = []
    for history_df in histories_df:
        n = history_df.shape[0]
        logging.info(f"History shape: {history_df.shape}")
        reg = LinearRegression()
        Xs = []
        ys = []
        for i in range(0, n-10):
            slice = history_df.iloc[i:i + 10, :][FEATURE_COLS]
            X = slice.to_numpy().flatten().tolist()
            Xs.append(X)
            y = history_df.iloc[i + 10]['High']
            ys.append(y)
        reg.fit(Xs, ys)
        models.append(reg)

    with open(MODELS_DIRECTORY, 'wb') as f:
        pickle.dump(models, f)


def make_prediction():
    """Make a prediction for today. """
    with open(MODELS_DIRECTORY, 'rb') as f:
        models = pickle.load(f)

    with open(DF_DIRECTORY, 'rb') as f:
        stock_dfs = pickle.load(f)

    predictions = []
    for i, stock_df in enumerate(stock_dfs):
        model = models[i]
        X = stock_df[FEATURE_COLS].to_numpy().flatten().reshape(1, -1)
        logging.info(X)
        prediction = model.predict(X)[0]
        predictions.append(prediction)

    with open(PREDICTION_DIREC, 'wb') as f:
        pickle.dump(predictions, f)


with DAG('stock',
         default_args=default_args,
         description='Stock Price Analysis for HW4',
         catchup=False,
         start_date=datetime(2021, 1, 1),
         schedule_interval='0 7 * * *',
         ) as dag:
    get_data_task = PythonOperator(task_id='get_data',
                                   python_callable=get_data)

    get_historical_data_task = PythonOperator(task_id='get_historic_data',
                                              python_callable=get_historical_data)

    make_prediction_task = PythonOperator(task_id="make_prediction",
                                          python_callable=make_prediction)

    train_model_task = PythonOperator(task_id='train_model',
                                      python_callable=train_models)

    compute_relative_error_task = PythonOperator(task_id="compute_relative_error",
                                                 python_callable=compute_relative_error)

    # task dependencies
    # (get_data_task >> get_historical_data_task >> train_model_task
    #  >> make_prediction_task >> compute_relative_error_task)
    (get_data_task >> get_historical_data_task >>
     train_model_task >> make_prediction_task >> compute_relative_error_task)
