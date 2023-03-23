from concurrent.futures import ThreadPoolExecutor, thread
from multiprocessing.pool import ThreadPool
from posixpath import split
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
import string

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
    time.sleep(2)

    t = driver.find_elements_by_xpath("//*[@id='root']/div[2]/div[2]/div[2]/div[2]/table/tbody/tr")
    br,sr = dict(),dict()
    for i in t:
        s = i.text.split(" ")
        ticker = s[3]
        sr[ticker] = s[4].split("%")[0]
        br[ticker] = s[5].split("%")[0]
        
    return (br,sr)

def get_mango(driver, url='https://trade.mango.markets/stats', assets=['SOL','USDC','USDT']):
    driver.get(url)
    driver.refresh()
    # supply_rates = driver.find_elements_by_css_selector('span.text-th-green')
    # borrow_rates = driver.find_elements_by_css_selector('span.text-th-red')
    
    t = driver.find_elements_by_xpath("//*[@class='min-w-full']/tbody/tr")
    br,sr = dict(),dict()
    for i in t:
        s = i.text.split(" ")
        if len(s) < 5:
            break
        ticker = s[0].split("\n")[0]
        sr[ticker] = s[2].split("%")[0]
        br[ticker] = s[3].split("%")[0]
    return (br, sr)
    
def get_port(driver, url='https://mainnet.port.finance/#/markets', assets=['USDC','USDT','UST','SOL','PAI']):
    driver.get(url)
    driver.refresh()
    driver.find_element_by_css_selector('button.close').click()
    time.sleep(2)

    t = driver.find_elements_by_xpath("//*[@id='market-asset-table']/tbody/tr")
    br,sr = dict(),dict()
    for i in t:
        s = i.text.split("\n")
        ticker = "".join([i for i in s[2] if i.isalpha()])
        try:
            br[ticker] = s[9].split("%")[0]
        except IndexError as e:
            br[ticker] = s[7].split("%")[0]
        sr[ticker] = s[6].split("%")[0]
        
        
    return (br, sr)

def get_solend(driver, url='https://solend.fi/dashboard', assets=['SOL','USDC','USDT','UST']):
    urls = ["https://solend.fi/dashboard?pool=turbo-sol", "https://solend.fi/dashboard?pool=stable"]
    driver.get(url)
    driver.refresh()
    time.sleep(3)
    pools = dict()
    
    t = driver.find_elements_by_xpath("//*[@id='root']/section/main/div/div/div[1]/div[2]/div[2]/div/div/div/div/div/div/table/tbody/tr")
    br,sr = dict(),dict()
    for i in t:
        s = i.text.split(" ")
        ticker = s[0].split("\n")[0]
        t0 = s[3].split("\n")
        try:
            t1 = s[4].split("\n")
        except IndexError:
            pass
        


        try:
            if t0[2].split("%")[0][1] == "x":
                sr[ticker] = t0[2].split("%")[0].split("x")[-1]
            else:
                sr[ticker] = t0[2].split("%")[0]

            if t1[2].split("%")[0][1] == "x":
                br[ticker] = t1[2].split("%")[0].split("x")[-1]
            else:
                br[ticker] = t1[2].split("%")[0]
        except IndexError as e:
            pass
    pools["main"] = br, sr
    for url in urls:
        driver.get(url)
        driver.refresh()
        time.sleep(2)
        t = driver.find_elements_by_xpath("//*[@id='root']/section/main/div/div/div[1]/div[2]/div[2]/div/div/div/div/div/div/table/tbody/tr")
        br,sr = dict(),dict()
        for i in t:
            s = i.text.split(" ")
            
            ticker = s[3].split("\n")[0]
            try:
                sup = s[3].split("\n")[2].split("%")[0]
                bor = s[4].split("\n")[-1].split("%")[0]
                if bor == "(":
                    bor = s[4].split("\n")[-2].split("x")[-1].split("%")[0]
                if sup == "+":
                    sup = s[4].split("\n")[0].split("%")[0]
                    bor = s[7].split("\n")[2].split("%")[0]
                elif bor == "+":
                    bor = s[5].split("\n")[0].split("%")[0]
                
            except IndexError as e:
                pass
            sr[ticker] = sup
            br[ticker] = bor
        pools[url.split("=")[-1]] = br,sr
    return pools

def get_francium(driver, url='https://francium.io/app/lend', assets=['USDC','USDT','UST','SOL','PAI']):
    driver.get(url)
    driver.refresh()
    time.sleep(2)

    t = driver.find_elements_by_xpath("//*[@id='app']/div/div[3]/div/div/div/div[2]/div/div[2]/div/div/div/div/div/div/table/tbody/tr")
    sr = dict()
    for i in t:
        s = i.text.split("\n")
        ticker = "".join([i for i in s[2] if i.isalpha()])
        sr[ticker] = s[7].split("%")[0]
    return sr

def get_tulip(driver, url='https://tulip.garden/lend', assets=['USDC','USDT','UST','SOL','PAI']):
    driver.get(url)
    driver.refresh()
    time.sleep(3)

    t = driver.find_elements_by_css_selector("div.lend-table__row-item")
    sr = dict()
    for i in t:
        s = i.text.split("\n")
        ticker = s[0]
        sr[ticker] = s[3].split("%")[0]
    return sr

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
    collection = db['lending']
    id = collection.insert_one(dict)
    return id

def update_document(new_dict, dex):
    
    client = connect_mongo()
    db = client['rates']
    collection = db['lending']
    myquery = { "dex": dex }
    new_dict['time'] = datetime.utcnow()
    newvalues = { "$set": new_dict }

    collection.update_one(myquery, newvalues)

    # collection.update_one({'_id':id}, {"$set": dict}, upsert=False)

def funding():
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
        # jet = get_jet(driver), "jet"
        # mango = get_mango(driver), "mango_apr"
        # port = get_port(driver), "port_apr"
        solend = get_solend(driver), "solend_apr" 
        # francium = get_francium(driver), "francium_apr"
        # tulip = get_tulip(driver), "tulip_apr"
        # anchor = get_anchor(driver), "anchor_apr"
        # d = {"supply":tulip[0]}
        # dict_to_mongo(d, "tulip")
        print(solend[0])

        # update_document(d, "jet")
        # print(solend)
        
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
        time.sleep(3600)
