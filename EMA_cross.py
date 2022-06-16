

import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame

import pandas as pd
import time
import datetime
from datetime import timedelta

from Alpaca_config import * # contains fmp key as well
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')

import logging
logging.basicConfig(filename='./new_5min_ema.log', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('{} logging started'.format(datetime.datetime.now().strftime("%x %X")))

def run_checker(Universe, fast, slow):
    print('run_checker started')
    clock = alpaca.get_clock()
    if clock.is_open: # add check if it is later than 10am
        end_dt = pd.Timestamp.now(tz='America/New_York') # change this to current time
        barset = {}
        idx = 0
        while idx <= len(Universe) - 1: # takes around 2 minutes
            for symbol in Universe[idx:idx+200]: # The maximum number of symbols we can request at once is 200
                bars = alpaca.get_bars(symbol, TimeFrame.Minute, end_dt.isoformat(), end_dt.isoformat(), limit=50, adjustment='raw').df
                bars['fast_ema'] = bars['close'].rolling(window=fast).mean() # 5
                bars['slow_ema'] = bars['close'].rolling(window=slow).mean() # 20
                barset[symbol] = bars
            idx += 200
        signals = {}
        for symbol in barset.keys():
            if barset[symbol].iloc[-1]['fast_ema'] > barset[symbol].iloc[-1]['slow_ema']: signal = 1
            else: signal = 0
            signals[symbol] = signal
        for signal in signals:
            if signals[signal] == 1:
                if signal not in [x.symbol for x in alpaca.list_positions()]:
                    logging.warning('{} {} - {}'.format(datetime.datetime.now(tz).strftime("%x %X"), signal, signals[signal]))
                    alpaca.submit_order(signal, 1, 'buy', 'market', 'day')
                    # print(datetime.datetime.now(tz).strftime("%x %X"), 'buying', signals[signal], signal)
            else:
                try:
                    alpaca.submit_order(signal, 1, 'sell', 'market', 'day')
                    logging.warning('{} {} - {}'.format(datetime.datetime.now(tz).strftime("%x %X"), signal, signals[signal]))
                except Exception as e:
                    # print('No sell', signal, e)
                    pass
        time.sleep(60) # Every minute
    else:
        time_to_open = clock.next_open - clock.timestamp
        print(f"Market is closed now going to sleep for {} minutes",format(time_to_open.total_seconds()//60))
        break

run_checker(stocks, 5, 20)