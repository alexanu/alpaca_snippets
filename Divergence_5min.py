# Source: https://github.com/proinvwin/My_5min_Strategy/blob/master/Divergence_5min.py



import alpaca_trade_api as tradeapi
import pandas as pd
import time
import datetime
from datetime import timedelta
import os
import requests
from bs4 import BeautifulSoup



hot_surge = 30
strong_surge = 20
min_price = 5
max_price = 330
avgVol_min = 500000
rsi_minimum = 25
track_qty = 50

hotVol_min = 120000
strongVol_min = 90000

market_value = 1.5

from utils import *
from Alpaca_config import * # contains fmp key as well
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
mail_subject = 'Testing Followed Stocks 5-min'
account = alpaca.get_account()


import logging
logging.basicConfig(filename='./new_5min_ema.log', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('{} logging started'.format(datetime.datetime.now().strftime("%x %X")))



def yahoo_url(stock):
    yahoo_1 = 'https://finance.yahoo.com/quote/'
    yahoo_2 = '?p='
    yahoo_3 = '&.tsrc=fin-tre-srch'
    ystock_path = yahoo_1+stock+yahoo_2+stock+yahoo_3
    return ystock_path 

def signal_num():
    market = yahoo_indexQuote() #================[float(sp_change[:-1]), float(dji_change[:-1]), float(nasdaq_change[:-1])]
    if market[0] >= market_value:
        Lmax = 0.005
        Lmin = 0.0015
        Smax = -0.002
        Smin = -0.0015
        Ldiv = 0.95
        Sdiv = 1.25
    elif 0 < market[0] < market_value:
        Lmax = 0.005
        Lmin = 0.0015
        Smax = -0.002
        Smin = -0.0015
        Ldiv = 0.95
        Sdiv = 1.1
    else:
        Lmax = 0.005
        Lmin = 0.0015
        Smax = -0.002
        Smin = -0.0015
        Ldiv = 1.25
        Sdiv = 0.95
    return [Lmax, Lmin, Smax, Smin, Ldiv, Sdiv]

   
def get_min_bars(symbols):
    data = api.get_barset(symbols, '1Min', limit=1000).df
    data.dropna(axis = 0, how ='any', inplace = True)
    #print(data)
    return data

def get_data_bars(symbols, rate, slow, fifty, mid, fast):
    data = api.get_barset(symbols, rate, limit=1000).df
    data.dropna(axis = 0, how ='any', inplace = True)
    #print(data)
    for x in symbols:
        data.loc[:, (x, 'slow_sma')] = data[x]['close'].rolling(window=slow).mean()

    #data.dropna(axis = 0, how ='any', inplace = True)    
    for x in symbols:      
        data.loc[:, (x, 'cl_divv')] = data[x]['low'] - data[x]['slow_sma'] 

    data.dropna(axis = 0, how ='any', inplace = True) 
    print(data)
    return data

def get_signal_bars(symbol_list, rate, sma_slow, sma_fifty, sma_mid, ema_fast):
    data = get_data_bars(symbol_list, rate, sma_slow, sma_fifty, sma_mid, ema_fast)
    #data.dropna(axis = 0, how ='any', inplace = True)
    signals = {}
    for x in symbol_list:
        mean_divv = data[x]['cl_divv'].mean()
        min_divv = data[x]['cl_divv'].min()
        max_divv = data[x]['cl_divv'].max()
        std_divv = data[x]['cl_divv'].std()
        datum = get_min_bars(x)
        print(datum)
        #price_Datum = datum.iloc[-1]['close']
        price_Datum = datum[x].iloc[-1]['close']
        current_slowSMA = data[x].iloc[-1]['slow_sma']
        current_divv = price_Datum - data[x].iloc[-1]['slow_sma']

######################################################################################################
        

        if (current_divv <= (min_divv*1.1)): signal = 6
        elif (current_divv <= (min_divv*1.05)): signal = 5
        elif (current_divv <= (min_divv)): signal = 4
        elif (current_divv <= (min_divv*0.95)): signal = 3
        elif (current_divv <= (min_divv*0.9)): signal = 2
        elif (current_divv <= (min_divv*0.85)): signal = 1

        elif (current_divv >= (max_divv*1.5)): signal = -7
        elif (current_divv >= (max_divv*1.375)): signal = -6
        elif (current_divv >= (max_divv*1.25)): signal = -5
        elif (current_divv >= (max_divv*1.15)): signal = -4
        #elif (current_divv >= (max_divv)): signal = -3
        #elif (current_divv >= (max_divv*0.90)): signal = -2
        #elif (current_divv >= (max_divv*0.8)): signal = -1
     
#####################################################################################################
 
            
        else: signal = 0
        signals[x] = signal
    return signals


def main_list():
    num = len(stocks)
    x = 0
    y = num
    main_list = []
    for i in range(x, y, 1):
        try:
            x = i
            chunks = stocks[x:x+1]
            main_list.append(chunks)
        except:
            pass
    return main_list

import fmpsdk # for fetching latest SP500 list
stocks = pd.json_normalize(fmpsdk.sp500_constituent(apikey=fmp_key)).symbol.to_list()
firstClass_Options = stocks[1:10]

n = 0
long_list = []
short_list = []
buy_dict = {}
sell_dict = {}
while True:
    #t = list(time.localtime())
    #if ((t[3])*100 +t[4]) >= 431 and int((t[3])*100 +t[4]) < 2100 and 0 <= datetime.datetime.today().weekday() <=4: 
    #my_message = f'Started' 
    #myEmail_Notify(my_message)
    mainlist = main_list()
    for i in range(len(mainlist)):
        try:      
            signals = get_signal_bars(mainlist[i], '5Min', 200, 50, 20, 8)
            #time.sleep(1)
            for signal in signals:
                if signals[signal] > 0: 
                    if analyst_Ratings(signal)[0] != 'Downgrades' or analyst_Ratings(signal)[1] != 'Sell' or analyst_Ratings(signal)[1] != 'Underweight':
                        if signal in buy_dict.keys():
                            if signals[signal] > buy_dict[signal]: 
                                if signal in firstClass_Options:   
                                    my_message = f'({signals[signal]}) : *** BUY {signal}; \n \n {yahoo_url(signal)}'
                                    sent_alpaca_email(mail_subject,my_message)
                                    buy_dict.update(signals)  
                                else:                                   
                                    my_message = f'({signals[signal]}) : BUY {signal}; \n \n {yahoo_url(signal)}'
                                    sent_alpaca_email(mail_subject,my_message)
                                    buy_dict.update(signals)
                        else:
                            if signal in firstClass_Options:   
                                my_message = f'({signals[signal]}) : *** BUY {signal}; \n \n {yahoo_url(signal)}'
                                sent_alpaca_email(mail_subject,my_message)
                                buy_dict.update(signals)  
                            else:
                                my_message = f'({signals[signal]}) : BUY {signal}; \n \n {yahoo_url(signal)}'
                                sent_alpaca_email(mail_subject,my_message)
                                buy_dict.update(signals)                      

                elif signals[signal] < 0:
                    #if analyst_Ratings(signal)[0] != 'Downgrades' or analyst_Ratings(signal)[1] != 'Sell' or analyst_Ratings(signal)[1] != 'Underweight':
                    if signal in sell_dict.keys():
                        if signals[signal] < sell_dict[signal]:  
                            if signal in firstClass_Options:            
                                my_message = f'({signals[signal]}) : *** SELL {signal}; \n \n {yahoo_url(signal)}'
                                sent_alpaca_email(mail_subject,my_message)
                                sell_dict.update(signals)
                            else:
                                my_message = f'({signals[signal]}) : SELL {signal}; \n \n {yahoo_url(signal)}'
                                sent_alpaca_email(mail_subject,my_message)
                                sell_dict.update(signals)                                
                    else:
                        if signal in firstClass_Options:          
                            my_message = f'({signals[signal]}) : *** SELL {signal}; \n \n {yahoo_url(signal)}'
                            sent_alpaca_email(mail_subject,my_message)
                            sell_dict.update(signals)
                        else:
                            my_message = f'({signals[signal]}) : SELL {signal}; \n \n {yahoo_url(signal)}'
                            sent_alpaca_email(mail_subject,my_message)
                            sell_dict.update(signals)          

        except:
            pass
    print(buy_dict)
    print(sell_dict)
    #time.sleep(160)