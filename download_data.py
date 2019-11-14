# extract_data.py
# Amir Harati 2019
"""
    Download data from different sources
    example:
    python download_data.py  --config config.yaml  --ticker_list tickers.list --exchange TSE --dist_dir test3  --morningstar --fyahoo_hist

    TODO:
    1- not save a file when tocker not existed.
    2- generate list of sucessful and falied 
    3- retry
"""
import urllib.request as urllib2
import re
import time
import random
import csv
import numpy as np
from datetime import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import shutil
import glob
import yfinance as yf
import argparse
import yaml
import  logging

#chrome_options = Options()  
def enable_download_in_headless_chrome(driver, download_dir):
    # add missing support for chrome "send_command"  to selenium webdriver
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    command_result = driver.execute("send_command", params)

def get_financials_morningstar(exchange, ticker, temp_dir, dist_dir):
    if exchange != "":
        site = "http://financials.morningstar.com/ratios/r.html?t=" + exchange + ":" + ticker
    else:
        site = "http://financials.morningstar.com/ratios/r.html?t=" + ticker

    fname = ticker + " Key Ratios.csv"
    fname_out = ticker + "_key.csv"
    driver.get(site)
    csv_button = WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH, '//a[@href="javascript:exportKeyStat2CSV();"]')))
    csv_button.send_keys("\n")
    while(os.path.exists(os.path.join(temp_dir,fname)) is False):
        pass
    time.sleep(0.01)
    shutil.move(os.path.join(temp_dir,fname), os.path.join(dist_dir,fname_out))
    return True
    

def get_fyahoo_historical(exchange, ticker, dist_dir):
    ticker = ticker.replace(".", "-")
    if exchange != "":
        combined_ticker = ticker + "." + exchange
    else:
        combined_ticker = ticker

    fname_out = ticker + "_hist.csv"
    x = yf.Ticker(combined_ticker)
    
    if len(x.info) > 0 :
        data = yf.download(combined_ticker, interval="1d",period="max", auto_adjust=True, actions=True, threads=8)
        data.to_csv(os.path.join(dist_dir, fname_out))
        return True
    else:
        logging.error("cant donwnload the data")
        raise Exception('ticker ' + combined_ticker + ' not avalible in yahoo finance')

def read_ticker_list(ticker_list):
    lines = [line.strip().split()[0] for line in open(ticker_list)]
    final_tickers = {}
    for line in lines:
        parts = line.split(".")
        if len(parts) == 1:
            final_tickers[line] = True
    for line in lines:
        parts = line.split(".")
        x = parts[0]
        c = 0
        if x not in final_tickers:
            while x not in final_tickers and c <= len(parts):
                x = ".".join(parts[0:c])
                c += 1
            final_tickers[x] = True
    #print(final_tickers)
    #exit(1)
    return list(final_tickers.keys())

def read_config(config_file):
    """
        read a yaml config file
    """
    conf = None
    with open(config_file, "r") as stream:
        try:
            # print(yaml.load(stream))
            conf = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc) 
    return conf


def map_exchange_morningstar(exchange):
    if exchange == "TSE":
        return "XTSE"
    elif exchange == "NYSE" or exchange == "NAS":
        return ""
    else:
        logging.error("exchnage " + exchange + " is not known!")
        exit(1)


def map_exchange_fyahoo(exchange):
    if exchange == "TSE":
        return "TO"
    elif exchange == "NYSE" or exchange == "NAS":
        return ""
    else:
        logging.error("exchnage " + exchange + " is not known!")
        exit(1)


parser = argparse.ArgumentParser(description='Download data')
parser.add_argument('--config', type=str, help='Path to experiment config.')
parser.add_argument('--log_level', default="INFO", type=str, help="log level")
parser.add_argument('--dist_dir', type=str, help='distnation directory')
parser.add_argument('--ticker_list', type=str, help='list of tickers')
parser.add_argument('--exchange', type=str, help='name of exchange')
parser.add_argument('--morningstar', action='store_true', help='download morningstar')
parser.add_argument('--fyahoo_hist', action='store_true', help='download yahoo historical')
#parser.add_argument('--report', type=str, help='report of downloads')
paras = parser.parse_args()

log_level = paras.log_level

log_numeric_level = getattr(logging, log_level.upper(), None)
if not isinstance(log_numeric_level, int):
    raise ValueError('Invalid log level: %s' % log_level)
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=log_numeric_level)

config = read_config(paras.config)
temp_dir = config["temp_dir"]
dist_dir = paras.dist_dir
tickers = read_ticker_list(paras.ticker_list)

os.makedirs(dist_dir, exist_ok=True)

chrome_options = webdriver.ChromeOptions()


chrome_options.add_argument('disable-component-cloud-policy')
prefs = {'download.prompt_for_download': False,
         'download.directory_upgrade': True,
         'download.default_directory': temp_dir}

chrome_options.add_experimental_option('prefs', prefs)
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)
enable_download_in_headless_chrome(driver, temp_dir)


# TODO:  
# do something for errors : (not found what ever)
# download options to download only needed source (from cmd)

success_morningstar = []
failed_morningstar = []
sucess_fyahoo = []
failed_fyahoo = []
for ticker in tickers:
    logging.info("downloading " + ticker)
    if paras.morningstar:
        try:
            get_financials_morningstar(map_exchange_morningstar(paras.exchange), ticker, temp_dir, dist_dir)
            success_morningstar.append(ticker)
        except:
            failed_morningstar.append(ticker)
            logging.error(ticker + " data from morningstar has not been downloaded")
    if paras.fyahoo_hist: 
        try:
            get_fyahoo_historical(map_exchange_fyahoo(paras.exchange), ticker, dist_dir)
            success_fyahoo.append(ticker)
        except:
            logging.error(ticker + " data from yahoo finance has not been downloaded")
            failed_fyahoo.append(ticker)