# process_data.py
# Amir Harati, Nov 2019
"""
    Code to process downloaded data.


    key file:
    This file has following general format:
    Each block is one category of data with year info.
    At the top is description of category , next is list of years (first column should be ignored)
    Next are bunch of rows for each varible in the category (it need to be processed since it might have extra info like currency)
    first column is the key, some values are missing. 

"""

import yaml
import logging
import numpy as np 
import pandas as pd

class ProcessData:
    def __init__(self):
        self._fyahoo_hist = {}

    def _read_ticker_list(self, ticker_list):
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
        return list(final_tickers.keys())

    def _read_config(self, config_file):
        """
            read a yaml config file
        """
        conf = None
        with open(config_file, "r") as stream:
            try:
                conf = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc) 
        return conf


    def _load_ticker_fyahoo_hist(self, ticker_id, ticker_file):
        self._fyahoo_hist[ticker_id] = pd.read_csv(ticker_file)

    def _load_ticker_morningstar_key(self, ticker_id, ticker_file):
        pass

    def _compute_yield(self):
        pass

    def _compute_value_ratios(self):
        pass

    def _compute_dividend_growth(self):
        pass

    def _compute_debt_ratios(self):
        pass
    
    def _compute_growth_ratios(self):
        pass

    def _filter_results(self):
        pass

    def _sort(self):
        pass

    