import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from sklearn.linear_model import LinearRegression
from datetime import datetime

FEATURE_COLS = ["Open", "Low", "Close", "Volume"]
PREDICTED_COL = ["High"]

def get_date_today():
    """Get date today in str format such as 20201119. """
    return datetime.today().strftime('%Y%m%d')

if __name__ == '__main__':
    # tickers = ['AAPL', 'GOOGL', 'FB', 'MSFT', 'AMZN']
    # for i in range(len(tickers)):
    #     df = yf.download(tickers[i],
    #                      period='10d')
    #     print(df)
    #     print(df.loc['20211119', 'High'])
        # reg = LinearRegression()
        # X = df[FEATURE_COLS].to_numpy()
        # y = df[PREDICTED_COL].to_numpy()
        # reg.fit(X, y)
        # print(y)
        # print(reg.predict(X))
    # print(get_date_today())
    # df = yf.download("GOOGL", period='10d')
    # print(df.iloc[-1, :]['High'])
    df = pd.DataFrame()
    df[get_date_today()] = 2934.2
    print(df)