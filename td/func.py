def get_options(ticker):
    options_sales = list()
    for sale in open(f"data/{ticker}.txt"):
        o = re.findall('(\d{1,2}\/\d{2}\/\d{2}) (\d{2}:\d{2}:\d{2})\s(\d{1,2}\s[A-Z]{3}\s[\d]{2})\s([\d?.\d+]*)\s([CP])\s(\d{1,3}(?:,\d{3})*|\d{4,})\s(-?(?:\.\d{1,2}|[1-9]\d*(?:\.\d{1,2})?))\s([A-Z0-9]+)\s(-?(?:\.\d{1,2}|[1-9]\d*(?:\.\d{1,2})?)x-?(?:\.\d{1,2}|[1-9]\d*(?:\.\d{1,2})?))\s(-?\d?.\d+)\s(\d+.\d+%|--)\s(\d+.\d+)(\s.+)?'
                       , sale)
        # print(o[0])
        o = o[0]

        if o[-1].strip() == 'Spread':
            # print('hi')
            dt = datetime.strptime(o[0] + " " + o[1], "%m/%d/%y %H:%M:%S")
            options_sales.append([ticker, o[2], o[3], o[4], dt, o[5], o[6], o[7], o[8], o[9], o[10], o[11], True])
        else:
            dt = datetime.strptime(o[0] + " " + o[1], "%m/%d/%y %H:%M:%S")
            options_sales.append([ticker, o[2], o[3], o[4], dt, o[5], o[6], o[7], o[8], o[9], o[10], o[11], False])
    df = pd.DataFrame(options_sales,
                      columns=['ticker','exp_date', 'strike', 'callput', 'time_of_sale', 'quantity',
                               'price', 'exchange', 'market', 'delta', 'implied_volatility', 'spot', 'spread'])
    return df
def copy_from_stringio(df, table):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, sep='|',index_label='id', header=False, index=False)
        buffer.seek(0)

        conn = psycopg2.connect(user="kg",
                                     password="welcome",
                                     host="192.168.0.46",
                                     port="5432",
                                     database="kg")

        cursor = conn.cursor()
        cursor.execute("SET search_path TO options_data")
        try:
            cursor.copy_from(buffer, table, sep="|")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("copy_from_stringio() done")
        cursor.close()
def get_from_db(ticker):
    conn = psycopg2.connect(user="kg",
                                     password="welcome",
                                     host="192.168.0.46",
                                     port="5432",
                                     database="kg")

    cursor = conn.cursor()
    cursor.execute(f"SELECT  exp_date, strike, time_of_sale, quantity, price, delta, implied_volatility, spot FROM options_data.sales WHERE ticker = '{ticker}'")

    tuples_list = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(tuples_list, columns=['exp_date','strike','time_of_sale','quantity','price','delta','implied_volatility','spot'])
    return df
def plot_spot_premium(ticker):
    df = get_from_db(ticker)
    premium = df[['exp_date','strike','time_of_sale','quantity','price','delta','implied_volatility','spot']]
    premium['quantity'] = pd.to_numeric(premium["quantity"].str.replace(",",""))
    premium["price"] = pd.to_numeric(premium["price"])
    premium["spot"] = pd.to_numeric(premium["spot"])
    premium['premium'] = premium.quantity * premium.price
    premium['time_of_sale'] = pd.to_datetime(premium['time_of_sale'])
    premium['premium'] = premium['premium'] * 100
    premium.set_index('time_of_sale', inplace=True)
    premium_minute = premium.resample('1T').agg({'spot': 'mean', 'quantity': 'sum', 'premium': 'sum'})

    # filter by premium
    premium_minute.loc[premium_minute['premium'] <= 50000000, 'premium'] = 0

    # Create the plot using matplotlib
    fig, ax = plt.subplots(figsize=(30, 30))
    ax.scatter(premium_minute.index, premium_minute['premium'], s=premium_minute['premium'] ** 0.5, alpha=0.5)
    ax.set_ylabel('Premium')
    ax2 = ax.twinx()
    ax2.plot(premium_minute.index, premium_minute['spot'], color='tab:blue')
    ax.set_xlabel('Time')
    ax2.set_ylabel('Spot Price')
    ax2.tick_params(axis='y', labelcolor='white')
    ax.legend(['Spot Price', 'Option Premium'])
    plt.show()
def import_new_data(ticker):
    df = get_options(ticker)
    copy_from_stringio(df, "sales")

def get_data(underlying: str, strikes: None, dates: None):
    url = f"https://api.tdameritrade.com/v1/marketdata/chains?&symbol={underlying}"
    page = requests.get(url=url,
                        params={'apikey': api_key})
    content = json.loads(page.content)
    df_list = list()
    for date in dates:
        dte = ""
        for d in content['callExpDateMap'].keys():
            if d[:10] == date: dte = d

        for strike in strikes:
            str_strike = str(strike) + ".0"
            try:
                opt_info = content['callExpDateMap'][dte][str_strike][0]
                info = ['symbol', 'bid', 'ask', 'mark', 'bidSize', 'askSize', 'totalVolume', 'volatility', 'delta',
                        'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue', 'daysToExpiration',
                        'intrinsicValue']
                for key in list(opt_info.keys()):
                    if key not in info: del opt_info[key]
                opt_info['timestamp'] = datetime.utcnow()
                df_list.append(opt_info)
            except KeyError:
                pass

            try:
                opt_info = content['putExpDateMap'][dte][str_strike][0]
                info = ['symbol', 'bid', 'ask', 'mark', 'bidSize', 'askSize', 'totalVolume', 'volatility', 'delta',
                        'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue', 'daysToExpiration',
                        'intrinsicValue']
                for key in list(opt_info.keys()):
                    if key not in info: del opt_info[key]
                opt_info['timestamp'] = datetime.utcnow()
                df_list.append(opt_info)
            except KeyError:
                pass

    return df_list

import pandas as pd
from datetime import datetime, timedelta
import tda
from selenium import webdriver
from collections import defaultdict
from io import StringIO


class TD:

    def __init__(self, api_key):
        self.api_key = api_key
        self.conn = None
        redirect_url = "https://localhost"
        token_path = "tokens"
        with webdriver.Chrome() as driver:
            self.client = tda.auth.client_from_login_flow(driver, self.api_key, redirect_url, token_path, asyncio=False,
                                                          token_write_func=None, enforce_enums=True)

    def copy_from_stringio(self, df, table):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, index_label='id', header=False, index=False)
        buffer.seek(0)
        if not self.conn: self.create_pg_conn()
        conn = self.conn
        cursor = conn.cursor()
        cursor.execute("SET search_path TO options_data")
        try:
            cursor.copy_from(buffer, table, sep=",")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("copy_from_stringio() done")
        cursor.close()

    def create_pg_conn(self):
        self.conn = psycopg2.connect(user="kg",
                                     password="welcome",
                                     host="192.168.0.46",
                                     port="5432",
                                     database="kg")

    def get_positions(self, acc_number):
        page = self.client.get_account(acc_number, fields=tda.client.Client.Account.Fields("positions"))
        # print(page.content)
        content = json.loads(page.content)
        positions = content['securitiesAccount']['positions']
        symbols = {i['instrument']['underlyingSymbol'] for i in positions}
        dfs = list()
        for sym in symbols:
            pos_sym_list = list()
            for pos in positions:
                if pos['instrument']['underlyingSymbol'] == sym:
                    pos_sym_list.append([pos['shortQuantity'], pos['longQuantity'], pos['instrument']['symbol'],
                                         pos['instrument']['putCall'], pos['averagePrice'], pos['marketValue']])

            df = pd.DataFrame(pos_sym_list,
                              columns=['shortQuantity', 'longQuantity', 'symbol', 'putCall', 'averagePrice',
                                       'marketValue'])
            df['quantity'] = df['longQuantity'] - df['shortQuantity']
            del df['longQuantity']
            del df['shortQuantity']
            dfs.append((sym, df))
        return dfs

    def analyze_positions(self):
        positions = get_positions()
        short = [i for i in positions if i['shortQuantity'] > 0]
        long = [i for i in positions if i['longQuantity'] > 0]
        exp = {re.findall('[0-9]{6}', i['instrument']['symbol'])[0] for i in positions}
        symbols = {i['instrument']['underlyingSymbol'] for i in positions}
        entries = parse_dfs(positions)

    def parse_dfs(self, dfs):
        l = list()
        for df in dfs:

            # net cost
            net_cost = 0
            side = "credit"
            for i in df[1].iterrows():
                if i[1][0] > 0:
                    net_cost += i[1][0] * -i[1][4]
                else:
                    net_cost += i[1][1] * i[1][4]
            net_cost = round(net_cost, 4)
            if net_cost > 0: side = "debit"
            l.append(df[0], net_cost, side)
        return l

    def get_data(self, underlying: str, strikes: None, dates: None):
        url = f"https://api.tdameritrade.com/v1/marketdata/chains?&symbol={underlying}"
        page = requests.get(url=url,
                            params={'apikey': api_key})
        content = json.loads(page.content)
        df_list = list()
        for date in dates:
            dte = ""
            for d in content['callExpDateMap'].keys():
                if d[:10] == date: dte = d
            for strike in strikes:
                str_strike = str(strike) + ".0"
                try:
                    opt_info = content['callExpDateMap'][dte][str_strike][0]
                    info = ['symbol', 'bid', 'ask', 'mark', 'bidSize', 'askSize', 'totalVolume', 'volatility', 'delta',
                            'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue', 'daysToExpiration',
                            'intrinsicValue']
                    for key in list(opt_info.keys()):
                        if key not in info: del opt_info[key]
                    opt_info['timestamp'] = datetime.now()
                    df_list.append(opt_info)
                except KeyError:
                    pass
                try:
                    opt_info = content['putExpDateMap'][dte][str_strike][0]
                    info = ['symbol', 'bid', 'ask', 'mark', 'bidSize', 'askSize', 'totalVolume', 'volatility', 'delta',
                            'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue', 'daysToExpiration',
                            'intrinsicValue']
                    for key in list(opt_info.keys()):
                        if key not in info: del opt_info[key]
                    opt_info['timestamp'] = datetime.utcnow()
                    df_list.append(opt_info)
                except KeyError:
                    pass

        return df_list

    def positions_data(self):
        current = td.get_positions("498199772")
        options_list = list()
        for position in current:
            date_dict = defaultdict(list)
            ticker = position[0]
            df = position[1]
            for i in list(df['symbol']):
                groups = re.findall("([A-Z]+)_(.{6})[PC](...)", i)[0]
                date = groups[1]
                date = f"20{date[-2:]}-{date[:2]}-{date[2:-2]}"
                date_dict[date].append(groups[2])
            d = dict(date_dict)
            strikes = [item for sublist in list(d.values()) for item in sublist]
            dates = d.keys()
            df = td.get_data(ticker, strikes, dates)
            options_list.append(df)

        options_detailed = current[0][1].merge(options_port, on='symbol', how='left').drop_duplicates()
        options_detailed['quantity'] = options_detailed['quantity'].astype(int)
        self.copy_from_stringio(options_detailed, "options_positions")

