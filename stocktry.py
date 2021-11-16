import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from sklearn.linear_model import LinearRegression

if __name__ == '__main__':
    tickers = ['AAPL', 'GOOGL', 'FB', 'MSFT', 'AMZN']
    for i in range(len(tickers)):
        df = yf.download(tickers[i],
                         period='10d')

