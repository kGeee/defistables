from concurrent.futures import ThreadPoolExecutor, thread
from multiprocessing.pool import ThreadPool
import re
from venv import create
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import sys,ssl
import time
import json
from datetime import datetime
from collections import defaultdict
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import re
import threading
import pymongo
import concurrent.futures

# from db_connection import Postgres
import uuid

# ARGS (optional): takes the url of the livetiming, number of queries, and the time between queries (seconds)
url = 'https://app.jetprotocol.io'# sys.argv[1] if (len(sys.argv) > 1) else 'https://livetiming.alkamelsystems.com/lcsc'
max_hits = 1#int(sys.argv[2]) if (len(sys.argv) > 2) else 3
interval = 1#int(sys.argv[3]) if (len(sys.argv) > 3) else 5

chromedriver_path = './chromedriver'

def get_jet(driver, url = 'https://app.jetprotocol.io', assets=['SOL','USDC']):
    driver.get(url)
    driver.refresh()
    driver.find_element_by_css_selector('i.jet-icons.close').click()
    time.sleep(3)
    sup = driver.find_elements_by_css_selector('tr')
    ra = [r.text[:-1] for r in sup][1:]
    
    supply = [i.split(' ')[5][:-1] for i in ra if i != '']
    borrow = [i.split(' ')[4][:-1] for i in ra if i != '']


    rates = pd.DataFrame(columns=['asset','jet_supply','jet_borrow'])
    rates['asset'] = assets
    rates['jet_supply'] = supply
    rates['jet_borrow'] = borrow
    return rates

def get_mango(driver, url='https://trade.mango.markets/stats', assets=['SOL','USDC','USDT']):
    driver.get(url)
    driver.refresh()
    supply_rates = driver.find_elements_by_css_selector('span.text-th-green')
    borrow_rates = driver.find_elements_by_css_selector('span.text-th-red')

    supply = [r.text[:-1] for r in supply_rates]
    borrow = [r.text[:-1] for r in borrow_rates]
    rates = pd.DataFrame(columns=['asset','mango_supply','mango_borrow'])
    rates['asset'] = assets

    rates['mango_supply'] = supply
    rates['mango_borrow'] = borrow
    return rates

def get_port(driver, url='https://mainnet.port.finance/#/markets', assets=['USDC','USDT','UST','SOL','PAI']):
    driver.get(url)
    driver.refresh()
    driver.find_element_by_css_selector('button.close').click()
    time.sleep(2)
    supply_rates = driver.find_elements_by_css_selector('div.td-deposit-apy-standard.text-green')
    borrow_rates = driver.find_elements_by_css_selector('td.text-end.text-red')
    supply = [r.text[:-1] for r in supply_rates]
    borrow = [r.text[:-1] for r in borrow_rates]
    rates = pd.DataFrame(columns=['asset','port_supply','port_borrow'])
    rates['asset'] = assets
    rates['port_supply'] = supply
    rates['port_borrow'] = borrow
    return rates

def get_solend(driver, url='https://solend.fi/dashboard', assets=['SOL','USDC','USDT','UST']):
    driver.get(url)
    driver.refresh()
    time.sleep(1)
    all_rates = driver.find_elements_by_css_selector('span.Typography_primary__r-t61.Typography_body__rkN69.Market_percent__1fdjc')
    sup_bor = [r.text[:-1] for r in all_rates]
    supply = sup_bor[::2]
    borrow = sup_bor[1::2]
    rates = pd.DataFrame(columns=['asset','solend_supply','solend_borrow'])
    rates['asset'] = assets
    rates['solend_supply'] = supply
    rates['solend_borrow'] = borrow
    return rates

def get_anchor(driver, url="https://app.anchorprotocol.com/"):
    driver.get(url)
    driver.refresh()
    time.sleep(3)
    rates = driver.find_elements_by_css_selector('tr')
    supply = [r.text[:-1] for r in rates]
    ra = supply[1].split('\n')
    rates = pd.DataFrame(columns=['asset','anchor_supply','anchor_borrow'])
    rates['asset'] = [ra[0]]
    rates['anchor_supply'] = ra[3][:-1]
    rates['anchor_borrow'] = ra[5]
    return rates

def create_driver():
    
    # options.add_experimental_option('excludeSwitches', [])
    s=Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)
    options = webdriver.ChromeOptions()
    options.headless = True
    driver.delete_all_cookies()
    driver.implicitly_wait(10)
    return driver

def connect_mongo():
    client = pymongo.MongoClient("mongodb://root:rootpassword@cluster.provider-0.prod.sjc1.akash.pub",30673)
    db = client.test
    return client

def dict_to_mongo(dict, dex):
    dict['time'] = datetime.utcnow()
    dict['dex'] = dex
    client = connect_mongo()
    db = client['rates']
    collection = db['funding']
    id = collection.insert_one(dict)
    return id


# pg = Postgres(host='cluster.provider-0.prod.ams1.akash.pub',port='31757',user="admin",pw="let-me-in",db="mydb")
def get_mango_funding(market=['SOL','ETH', 'AVAX', 'LUNA', 'BTC']):
    rates = dict()
    driver = create_driver()

    for instrument in market:
        driver.get("https://trade.mango.markets/?name=" + instrument + "-PERP")
        # driver.refresh()
        time.sleep(3)
        funding_rate = driver.find_elements_by_xpath('.//div[@class = "text-th-fgd-1 md:text-xs"]')
        funding = [r.text for r in funding_rate]
        # rate = re.search("[\d.\d]*",funding[2])

        rates[instrument] = float(funding[2][:7].strip("%"))
    driver.quit()

    print(f"Mango: {rates}")
    print(dict_to_mongo(rates, "mango"))
    return rates

def get_drift_funding(market=['SOL','ETH', 'AVAX', 'LUNA', 'BTC']):
    rates = dict()
    driver = create_driver()
    for instrument in market:
        driver.get("https://app.drift.trade/" + instrument)
        # driver.refresh()
        time.sleep(3)
        funding_rate = driver.find_elements_by_xpath('.//span[@class = "font-semibold text-xs"]')
        funding = [r.text for r in funding_rate]
        rates[instrument] = float(funding[0][:-1])
        # rates = pd.DataFrame(columns=['asset','rate'])
        # rates['asset'] = assets
        # rates['rate'] = funding
    driver.quit()

    print(f"Drift: {rates}")
    print(dict_to_mongo(rates, "drift"))
    return rates

def get_01_funding(market=['SOL','ETH', 'AVAX', 'LUNA', 'BTC']):
    rates = dict()
    driver = create_driver()
    for instrument in market:
        driver.get("https://01.xyz/trade/?market=" + instrument + "-PERP")
        # driver.refresh()
        time.sleep(3)
        funding_rate = driver.find_elements_by_xpath('.//span[@class = "font-bold text-secondary-200 font-roboto"]')
        funding = [r.text for r in funding_rate]
        rates[instrument] = float(funding[1][:-1])
        # rates = pd.DataFrame(columns=['asset','rate'])
        # rates['asset'] = assets
        # rates['rate'] = funding
    driver.quit()

    print(f"01 : {rates}")
    print(dict_to_mongo(rates,"01"))
    return rates

def find_optimal(fr,market=['SOL','ETH', 'AVAX', 'LUNA', 'BTC']):
    # input : list of dictionary of funding rates
    rates = dict()
    for i in market:
        rates[i] = list()
    
    for dex in fr:
        for k,v in dex.items():
            if k in market:
                rates[k].append((dex['dex'], v))


    for k,v in rates.items():
        v.sort(key=lambda x: x[1])
        min = v[0]
        max = v[-1]
        print(k, min, max, min+max)

if __name__ == "__main__":
    while True:
        
        driver = create_driver()

        # Borrow Rates
        jet = get_jet(driver), "jet_apr"
        mango = get_mango(driver), "mango_apr"
        port = get_port(driver), "port_apr"
        solend = get_solend(driver), "solend_apr"
        anchor = get_anchor(driver), "anchor_apr"

        print(jet)
        
        # Funding Rates
        # thread_list = []

        # with ThreadPoolExecutor(max_workers=3) as executor:
        #     thread_list.append(executor.submit(get_01_funding))
        #     thread_list.append(executor.submit(get_mango_funding))
        #     thread_list.append(executor.submit(get_drift_funding))
        #     # funding = get_01funding(driver)
        #     find_optimal([i.result() for i in thread_list])
        #     time.sleep(300)

        
        # protocols = [jet,mango, port, solend, anchor]
        # for protocol, table_name in protocols:
        #     conn = pg.connect()
        #     protocol['time'] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        #     protocol['time_uuid'] = uuid.uuid4().hex
        #     pg.execute_many(conn, protocol, table_name)
        #     print("waiting 1 hr before fetching again")
        # time.sleep(3600)
