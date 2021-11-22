from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import yfinance as yf
import pickle
import pandas as pd
from sklearn.linear_model import LinearRegression

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
    update csv. """
    first_day = False
    try:
        with open(MODELS_DIRECTORY, 'rb') as f:
            models = pickle.load(f)

        relative_errors = pd.read_csv(RELATIVE_ERRORS_DIREC, index_col=0)
    except FileNotFoundError:
        print("No files for models.")
        first_day = True
        pass

    stock_dfs = []
    for i, company in enumerate(COMPANIES):
        df = yf.download(company, period='10d')
        if not first_day:
            model = models[i]
            X = df[FEATURE_COLS].to_numpy()
            # high_today = df.loc[get_date_today(), 'High']
            high_today = df.iloc[-1, :]['High']
            relative_error = (model.predict(X) - high_today) / high_today
            relative_errors[get_date_today()] = relative_error
            relative_errors.to_csv(RELATIVE_ERRORS_DIREC)
        else:
            # intialize the csv file
            relative_errors = pd.DataFrame()
            relative_errors.to_csv(RELATIVE_ERRORS_DIREC)
        stock_dfs.append(df)
    with open(DF_DIRECTORY, 'wb') as f:
        pickle.dump(stock_dfs, f)


def make_prediction():
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
                                          python_callable=make_prediction)

    # task dependencies
    get_data_task >> make_prediction_task
