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
import re

class ProcessData:
    def __init__(self):
        # define patterns
        pat_fin = "Financials"
        self.pat_fin = re.compile(pat_fin)

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
        FIN_DATA = {}

        # define a simple finite state machine to parse the file
        state = "INIT"
        lines = [line.strip() for line in open(ticker_file)]
        #ASSUMING orders are always the same. Otherwise need to do more pattern matchings
        for line in lines:
            m = self.pat_fin.match(line)
            if m is not None:
                state = "FIN" # 
                continue
            elif state == "FIN":
                state = "FIN-YEAR"
                fin_years = line.split(",")[1:]
                FIN_DATA["years"] = fin_years
                continue
            elif state == "FIN-YEAR":
                state = "FIN-REV"
                parts = line.splir(",")
                curr = " ".join(parts[0].split()[1:])
                FIN_DATA["REV_CUURENCY"] = curr
                FIN_DATA["REV"] = parts[1:]
                continue
            elif state == "FIN-REV":
                state = "FIN-GROSS"
                pass # TODO: parse
                continue
            elif state == "FIN-GROSS":
                state = "FIN-OPINC"
                pass # TODO
                continue
            elif state == "FIN-OPINC":
                state = "FIN-OPMARG"
                pass
                continue
            elif state == "FIN-OPMARG"
                state = "FIN-NETINC"
                pass
                continue
            elif state == "FIN-NETINC":
                state = "FIN-EPS"
                pass
                continue
            elif state == "FIN-EPS":
                state = "FIN-DIV"
                pass
                continue
            elif state == "FIN-DIV":
                state = "FIN-PAYOUT"
                pass
                continue
            elif state == "FIN-PAYOUT"
                state = "FIN-SHARE"
                pass
                continue
            elif state == "FIN-SHARE":
                state = "FIN-BPS"
                pass
                continue
            elif state == "FIN-BPS":
                state = "FIN-OPCASH"
                pass
                continue
            elif state == "FIN-OPCASH":
                state = "FIN-CAPSPEND"
                pass
                continue
            elif state == "FIN-CAPSPEN":
                state = "FIN-FREECASH"
                pass
                continue
            elif state == "FIN-FREECASH":
                state = "FIN-FRECASHSHARE" # free cash per share 
                pass
                continue
            elif state == "FIN-FREECASHSHARE":
                state = "FIN-WOCAP"
                pass
                continue
            





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

    