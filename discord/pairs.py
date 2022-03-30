from datetime import date, timedelta
import ccxt
from pprint import pprint
import matplotlib.pyplot as plt
import pandas as pd
import os

class Pairs:
    def __init__(self):
        pass

    def get_historical_data(self, since, market, resolution):
        exchange = ccxt.ftx({'enableRateLimit': True})
        since = exchange.parse8601(f"{since}T00:00:00Z")
        params = {'market_name': market}  # https://github.com/ccxt/ccxt/wiki/Manual#overriding-unified-api-params
        limit = None
        # specify any existing symbol here ↓ (it does not matter, because it is overrided in params)
        ohlcv = exchange.fetch_ohlcv(market, resolution, since, limit, params)
        return ohlcv

    def download_historical_data(self, start_date, ticker, resolution):
        data = self.get_historical_data(start_date, ticker, resolution)
        file_name = f"data/{ticker}_{resolution}_{start_date}.csv"
        df = pd.DataFrame(data, columns=['time','open','high','low','close','volume'])
        df.to_csv(file_name)
        return file_name

    def read_historical_data(self,start_date, ticker, resolution):
        data = pd.read_csv(f"data/{ticker}_{resolution}_{start_date}.csv")
        return pd.DataFrame(data, columns=['time','open','high','low','close','volume'])

    def delete_data(self):
        for f in os.listdir("data"):
            os.remove(f"data/{f}")

    def index(self, weights, resolution='1h', lookback_window = 30, starting_balance = 1000):
        start_date = date.today() - timedelta(lookback_window)
        ohlc_data = dict()
        holding = dict()
        for ticker, weight in weights.items():
            try:
                ohlc = self.read_historical_data(start_date, f"{ticker}-PERP", resolution)
            except FileNotFoundError as e:
                ohlc_filename = self.download_historical_data(start_date, f"{ticker}-PERP", resolution)
                ohlc = self.read_historical_data(start_date, f"{ticker}-PERP", resolution)
            
            holding[ticker] = weight * starting_balance / ohlc['open'][0]
            ohlc['return'] = ohlc['close'] / ohlc['open'][0]
            # if holding[ticker] < 0:
            #     ohlc['value'] = weight * starting_balance * (-1/ohlc['return'])
            #     ohlc['pnl'] = ohlc['value'] + weight*starting_balance

            # else:
            #     ohlc['value'] = weight * starting_balance * ohlc['return']
            #     ohlc['pnl'] = ohlc['value'] - weight*starting_balance


            ohlc['value'] = starting_balance + (weight * starting_balance * ohlc['return'])
            ohlc['pnl'] = ohlc['value'] - weight*starting_balance
            ohlc_data[ticker] = ohlc
        windowlength = len(list(ohlc_data.values())[0]) - 1
        va = [0]*(windowlength)
        plt.figure(figsize=(15,10))
        for k,v in ohlc_data.items():
            for i in range(len(va)):
                va[i] += v['pnl'][i]
            # plt.plot(v['return'], label = k)
        pct_return = [(i/starting_balance) + 1 for i in va]
        
        pt = [(i/va[0]-1) for i in va]

        ax = plt.axes()
        ax.set_facecolor("white")
        ax.plot(pt, color='black', label='return')
        ax.legend()

        # plt.plot(pct_return, color='black', label='return')
        plt.savefig('index.png')

        text = f"min drawdown: {round(100*min(pt),2)}% \nmax return: {round(100*max(pt),2)}% \ncurrent return: {round(100*pt[-1],2)}%"


        return ohlc_data, va, text
