import ccxt
import time
from concurrent.futures import ThreadPoolExecutor, thread
import concurrent.futures
from matplotlib.pyplot import axis
from numpy import size
import os
from termcolor import colored
import pandas as pd
from prettytable import PrettyTable


class OMS:
    def __init__(self):

        ftx = ccxt.ftx({
                            'apiKey': "",
                            'secret': "",
                            'enableRateLimit': True,
                        })
                

        self.oms =  ftx

    def scale(self, price_range:tuple, num_orders:int):
        """
        price_range : Range of prices to be filled ex. (10,20)
        num_orders : number of orders
        """
        step = (price_range[1] - price_range[0]) / num_orders
        prices = list()
        for i in range(num_orders):
            value = price_range[0] + 2.75*i*step
            prices.append(value)
            step -= step/num_orders
        return prices

    def buy_range(self, symbol:str, b_range:tuple, size:int, num_orders:int):
        """
        symbol : Symbol for market
        b_range: Buy range
        size : size to be filled
        num_orders : amount of orders to fill
        """
        buy = self.scale(b_range, num_orders=num_orders)
        unit_size = size / len(buy)
        for i, price in enumerate(buy):
            self.oms.create_order(symbol=symbol, side="buy", amount = unit_size, type="takeProfit", price=price, params={"triggerPrice":price})

    def sell_range(self, symbol, s_range, size, num_orders):
        """
        symbol : Symbol for market
        s_range: Sell range
        size : size to be filled
        num_orders : amount of orders to fill
        """
        sell = self.scale(s_range, num_orders=num_orders)
        unit_size = size / len(sell)
        for i, price in enumerate(sell):
            self.oms.create_order(symbol=symbol, side="sell", amount = unit_size, type="takeProfit", price=price, params={"triggerPrice":price})

    def range(self, side, symbol, range, size, num_orders):
        scale = self.scale(range, num_orders=num_orders)
        unit_size = size / len(scale)
        for i, price in enumerate(scale):
            self.oms.create_order(symbol=symbol, side=side, amount = unit_size, type="takeProfit", price=price, params={"triggerPrice":price})

    def cancel_all_orders(self, symbol):
        """
        symbol : Symbol for market
        """
        self.oms.cancel_all_orders( symbol, {'conditionalOrdersOnly': 'true'})

    def create_grid(self, symbol, sell_range, buy_range, size, num_orders):
        """
        symbol : Symbol for market
        sell_range: Sell range
        buy_range: Buy range
        size : size to be filled
        num_orders : amount of orders to fill
        """

        self.range("buy", symbol, buy_range, int(size/2), int(num_orders/2))
        self.range("sell", symbol, sell_range, int(size/2), int(num_orders/2))

    def balance(self):
        df = pd.DataFrame(self.oms.fetch_balance()['info']['result'])
        sum = 0
        for i in df['usdValue']:
            sum += float(i)
        return sum

    def bid_ask(self, markets):
        mkts = pd.DataFrame(self.oms.fetch_markets())
        bid_ask = dict()
        for i in markets:
            s = i + "/USD:USD"
            last = mkts.query("symbol == @s")
            bid_ask[i] = (last['info'].iloc[0]['bid'], last['info'].iloc[0]['ask'])
        return bid_ask

    def chase_limit(self, markets):
        bid_ask = self.bid_ask(markets)
        print(bid_ask)



    def last_prices(self, markets):
        mkts = pd.DataFrame(self.oms.fetch_markets())

        last_prices = dict()
        for i in markets.keys():
            s = i + "/USD:USD"
            last = mkts.query("symbol == @s")
            # print(last['info'].iloc[0]['last'])
            last_prices[i] = (last['info'].iloc[0]['last'], last['info'].iloc[0]['minProvideSize'])
        return last_prices

    def twap_df(self, df):
        def side(row):
            if row['diff'] <= 0:
                return "sell"
            else:
                return "buy"

        duration = int(input("wait time between orders in seconds: "))
        orders = int(input("orders: "))
        df['unit_size'] = df['diff'].div(orders)
        df['side'] = df.apply(lambda row: side(row), axis=1)
        print(list(df.ticker))

        executed_orders = 0
        while executed_orders < orders:
            for i in df.iterrows():
                # print(i[1])
                color = "red" if i[1]['side'] == "sell" else "green"
                
                print(colored(f"{i[1]['side']}ing", color), colored(round(abs(i[1]['unit_size']),4), "cyan"), colored(i[1]['ticker'] + "-PERP" ,"yellow"))
                self.oms.create_market_order(symbol=i[1]['ticker'] + "-PERP", side=i[1]['side'], amount=abs(i[1]['unit_size']))
            executed_orders += 1
            time.sleep(duration)



    def twap(self, side:str, symbol:str, size:float, duration:int, orders:int = 20):
        """
        side : side to fill - "buy" or "sell"
        symbol : Symbol for market
        size : size to be filled
        duration : time to fill size in minutes
        orders : amount of orders to fill
        """
        # splits total_size into equal sizes to fill in given duration
        unit_size = size / orders
        sleep_duration = duration / orders * 60
        executed_orders = 0
        color = "red" if side=="sell" else "green"
        while executed_orders < orders:
            print(colored(f"{side}ing", color), colored(symbol,"yellow"), "size", colored(unit_size, "cyan"))
            self.oms.create_market_order(symbol=symbol, side=side, amount=unit_size)
            executed_orders += 1
            time.sleep(sleep_duration)

        print(f"{side} twap for {size} {symbol} completed")

    def ls_pair(self):
        """
        buy_symbol : Symbol to buy
        sell_symbol : Symbol to sell
        buy_size : Buy size to be filled
        sell_size : Sell size to be filled
        duration : time to fill size in minutes
        orders : amount of orders to fill
        """
        buy_symbol, buy_size = input("Ticker to long: ") + "-PERP", input("Size to long: ")
        sell_symbol, sell_size = input("Ticker to short: ") + "-PERP", input("Size to long: ")
        duration, orders = input("How long to twap in minutes: "), input("Number of orders: ")


        print(f"twapping into {buy_symbol} / {sell_symbol} for {duration} minutes")
        thread_list = []
        def buy():
            self.twap(side="buy", symbol=buy_symbol, size=buy_size, duration=duration, orders=orders)
        def sell():
            self.twap(side="sell", symbol=sell_symbol, size=sell_size, duration=duration, orders=orders)

        with ThreadPoolExecutor(max_workers=2) as executor:
            thread_list.append(executor.submit(buy))
            thread_list.append(executor.submit(sell))

    def reduce(self, symbol, pct):
        pass

    def ls_index(self, weights={}):
        sz = input(f"# of tickers: ")
        total_notional = float(input(f"Total Notional to enter ($): "))
        duration = int(input("Duration to enter (Minutes): "))
        orders = int(input("Total number of orders: "))
        if len(weights) == 0:
            for i in range(int(sz)):
                symbol = input("Ticker: ")
                weight = float(input("Weight: "))
                weights[symbol.upper()+"-PERP"] = weight

        sleep_duration = duration / orders * 60
        executed_orders = 0
        while executed_orders < orders:
            markets = self.oms.load_markets()

            for symbol, weight in weights.items():
                value = ((weight * total_notional) / orders) / round(float(markets[symbol[:-5]+"/USD"]['info']['price']),8)
                size_increment = float(markets[symbol[:-5]+"/USD"]['info']['sizeIncrement'])
                size = int(value / size_increment) * size_increment
                side = "buy" if weight > 0 else "sell"
                color = "red" if side=="sell" else "green"
                print(colored(f"{side}ing", color), colored(symbol,"yellow"), "size", colored(size, "cyan"))
                self.oms.create_market_order(symbol=symbol, side=side, amount=abs(size))
            executed_orders += 1
            if executed_orders == orders:
                print("index entry complete")
                return
            time.sleep(sleep_duration)

    def buy_percentage(self, symbol):
        print(self.oms.fetch_orders(symbol=symbol))

    def bump(self, ticker, bump_value):
        # get trigger orders of ticker
        # submit modified query with bump
        pass

    def fetch_account_balance(self):
        balance = self.oms.fetch_balance()
        balances = [[i['coin'],i['usdValue']]for i in balance['info']['result'] if float(i['usdValue']) > 0.01]
        # print(sum([round(float(i[1]),2) for i in balances]))
        return sum([round(float(i[1]),2) for i in balances])

    def positions(self):
        t = PrettyTable(['Symbol', 'Side', 'Weight (%)', 'Contracts', 'Notional ($)', 'uPnL'])

        positions = pd.DataFrame(self.oms.fetch_positions())
        p = positions[positions['notional'] > 0][['symbol','notional','side','contracts','unrealizedPnl']]
        total = p['notional'].sum()
        p['weight'] = round(p['notional'] / total,2) * 100
        for i in p.sort_values(by=["weight"], ascending=False).iterrows():
            color = "red" if i[1]['side'] == "short" else "green"
            pnl = "red" if i[1]['unrealizedPnl'] <= 0 else "green"
            t.add_row([colored(i[1]['symbol'].removesuffix('/USD:USD'), 'yellow'), colored(i[1]['side'], color), colored(i[1]['weight'],"yellow"), colored(i[1]['contracts'], 'yellow'), colored(i[1]['notional'],"yellow"),  colored(i[1]['unrealizedPnl'], pnl)])
        account_value = self.fetch_account_balance()
        print(t)
        print(colored(f"Total Leverage: {total / account_value}", "yellow"))
        print(colored(f"Total $: {total}", "yellow"))
        return p

    def pnl(self):
        positions = pd.DataFrame(self.oms.fetch_positions())
        p = positions[positions['notional'] > 0][['symbol','notional','side','unrealizedPnl']]
        pnl = p['unrealizedPnl'].sum()
        return pnl, p

    def scale_tranches(self, side:str, symbol:str, ranges:list, size:float, orders_per_tranche:int, total_tranches:int):
        tranche_spread = abs(ranges[0] - ranges[1]) / total_tranches
        tranche = list()
        start_range = ranges[0]
        for i in range(total_tranches):

            if (side == "buy"):
                end_range = start_range-tranche_spread
            else:
                end_range = start_range+tranche_spread
            tranche.append([start_range, end_range])
            start_range = end_range
        for tranche_range in tranche:
            self.range(side, symbol, tranche_range, size/total_tranches, orders_per_tranche)

    def net_lev(self):
        net_lev = 0.0
        value = self.balance()
        pos = self.positions()
        for i in pos.iterrows():
            if i[1]['side'] == "short":
                net_lev -= i[1]['notional'] / value
            else:
                net_lev += i[1]['notional'] / value
        print("Net Leverage :", net_lev)

    def rebalance(self, markets={}):
        # rebalance account to weights portfolio, if empty ask user for input
        
        if markets=={}:
            sz = input("Number of tickers: ")
            for i in range(int(sz)):
                ins = input("Ticker: ")
                w = float(input("Weight: "))
                markets[ins.upper()] = w

        pos = self.positions()
        # pos = o.positions()
        lev = float(input("Leverage: "))
        lp = self.last_prices(markets)
        value = self.balance()
        size = dict()

        # calculate desired size
        for k,v in markets.items():
            notional = v*value*lev            
            amt = notional / float(lp[k][0])
            sz = int(amt / float(lp[k][1])) * float(lp[k][1])
            size[k] = sz

        sz_pd = pd.DataFrame([[k,v] for k,v in size.items()], columns=["ticker","size"])

        def size(row):
            if row['side'] == "short":
                return -row['contracts']
            else:
                return row['contracts']

        def rm_suffix(row):
            return row['symbol'].removesuffix("/USD:USD")

        curr = pd.DataFrame()
        if pos.size != 0:
            pos['curr_size'] = pos.apply (lambda row: size(row), axis=1)
            pos['ticker'] = pos.apply (lambda row: rm_suffix(row), axis=1)
            curr['curr_size'] = pos.apply (lambda row: size(row), axis=1)
            curr['ticker'] = pos.apply (lambda row: rm_suffix(row), axis=1)

        final = pd.DataFrame()
        if curr.size != 0:
            final = pd.merge(sz_pd, curr, on="ticker", how="outer").fillna(0)
            final['diff'] = final['size'] - final['curr_size']
            self.twap_df(final)
        else:
            final['ticker'] = sz_pd['ticker']
            final['diff'] = sz_pd['size']
            self.twap_df(final)
        self.net_lev()
        # print(pos.apply (lambda row: size(row), axis=1))

