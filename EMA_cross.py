

import alpaca_trade_api as tradeapi
import time
import datetime
from datetime import timedelta
from pytz import timezone
tz = timezone('America/New_York')

from Alpaca_config import * # contains fmp key as well
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')

import logging
logging.basicConfig(filename='./new_5min_ema.log', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('{} logging started'.format(datetime.datetime.now().strftime("%x %X")))

def get_data_bars(symbols, rate, slow, fast):
    data = api.get_barset(symbols, rate, limit=20).df
    for x in symbols:
        data.loc[:, (x, 'fast_ema')] = data[x]['close'].rolling(window=fast).mean()
        data.loc[:, (x, 'slow_ema')] = data[x]['close'].rolling(window=slow).mean()
    return data

data = api.get_barset(stocks, '1Min', limit=100).df

for x in stocks:
    data.loc[:, (x, 'fast_ema')] = data[x]['close'].rolling(window=20).mean()
    data.loc[:, (x, 'slow_ema')] = data[x]['close'].rolling(window=80).mean()

data.to_csv('dt.csv')
signals = {}

for x in stocks:
    if data[x].iloc[-1]['fast_ema'] > data[x].iloc[-1]['slow_ema']: signal = 1
    else: signal = 0
    signals[x] = signal

data['AA'].iloc[-1]['fast_ema']


def get_signal_bars(symbol_list, rate, ema_slow, ema_fast):
    data = get_data_bars(symbol_list, rate, ema_slow, ema_fast)
    signals = {}
    for x in symbol_list:
        if data[x].iloc[-1]['fast_ema'] > data[x].iloc[-1]['slow_ema']: signal = 1
        else: signal = 0
        signals[x] = signal
    return signals


def check_market_open():
    clock = alpaca.get_clock()
    if clock.is_open:
        pass
    else:
        time_to_open = clock.next_open - clock.timestamp
        print(f"Market is closed now going to sleep for {time_to_open.total_seconds()//60} minutes")
        time.sleep(time_to_open.total_seconds())

def run_checker(stocklist):
    print('run_checker started')
    while True:
        check_market_open()
        signals = get_signal_bars(stocklist, '5Min', 20, 5)
        for signal in signals:
            if signals[signal] == 1:
                if signal not in [x.symbol for x in api.list_positions()]:
                    logging.warning('{} {} - {}'.format(datetime.datetime.now(tz).strftime("%x %X"), signal, signals[signal]))
                    api.submit_order(signal, 1, 'buy', 'market', 'day')
                    # print(datetime.datetime.now(tz).strftime("%x %X"), 'buying', signals[signal], signal)
            else:
                try:
                    api.submit_order(signal, 1, 'sell', 'market', 'day')
                    logging.warning('{} {} - {}'.format(datetime.datetime.now(tz).strftime("%x %X"), signal, signals[signal]))
                except Exception as e:
                    # print('No sell', signal, e)
                    pass
        time.sleep(60) # Every minute

stocks = ['AA','AAL','AAPL','AIG','AMAT','AMC','AMD','AMGN','AMZN','APA','BA','BABA','BAC','BBY','BIDU','BP','C','CAT','CMG',]

run_checker(stocks)