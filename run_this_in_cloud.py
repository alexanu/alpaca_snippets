
# https://github.com/McKlayne/automate_algo_trading_medium/blob/master/Pairs_Trading_Algo_Example.py
# Medium: https://medium.com/automation-generation/algorithmic-trading-automated-in-python-with-alpaca-google-cloud-and-daily-email-notifications-422b7c6b7c53


import os
import datetime as dt
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
import pandas as pd
from utils_for_alpaca import *
from Alpaca_config import *

alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
mail_subject = 'Testing Followed Stocks 5-min'


def pairs_trading_algo(self):
    
    days = 1000 # working days
    stock1 = 'ADBE'
    stock2 = 'AAPL'
    stock1_barset = alpaca.get_bars(stock1, TimeFrame.Day,n_days_ago,today,adjustment='raw').df
    stock2_barset = alpaca.get_bars(stock2, TimeFrame.Day,n_days_ago,today,adjustment='raw').df
    stock1_mavg = stock1_hist.mean()
    stock2_mavg = stock2_hist.mean()
    spread_avg = min(stock1_mavg - stock2_mavg)
    #Spread_factor
    spreadFactor = .01
    wideSpread = spread_avg*(1+spreadFactor)
    thinSpread = spread_avg*(1-spreadFactor)
    #Calc_of_shares_to_trade
    cash = float(account.buying_power)
    limit_stock1 = cash//stock1_curr
    limit_stock2 = cash//stock2_curr
    number_of_shares = int(min(limit_stock1, limit_stock2)/2)
    
    #Trading_algo
    portfolio = api.list_positions()
    clock = api.get_clock()
    
    if clock.is_open == True:
        if bool(portfolio) == False:
            #detect a wide spread
            if spread_curr > wideSpread:
                #short top stock
                api.submit_order(symbol = stock1,qty = number_of_shares,side = 'sell',type = 'market',time_in_force ='day')
                #Long bottom stock
                api.submit_order(symbol = stock2,qty = number_of_shares,side = 'buy',type = 'market',time_in_force = 'day')
                mail_content = "Trades have been made, short top stock and long bottom stock"
            #detect a tight spread
            elif spread_curr < thinSpread:
                #long top stock
                api.submit_order(symbol = stock1,qty = number_of_shares,side = 'buy',type = 'market',time_in_force = 'day')
                #short bottom stock
                api.submit_order(symbol = stock2,qty = number_of_shares,side = 'sell',type = 'market',time_in_force ='day')
                mail_content = "Trades have been made, long top stock and short bottom stock"
        else:
            wideTradeSpread = spread_avg *(1+spreadFactor + .03)
            thinTradeSpread = spread_avg *(1+spreadFactor - .03)
            if spread_curr <= wideTradeSpread and spread_curr >=thinTradeSpread:
                api.close_position(stock1)
                api.close_position(stock2)
                mail_content = "Position has been closed"
            else:
                mail_content = "No trades were made, position remains open"
                pass
    else:
        mail_content = "The Market is Closed"
        
    sent_alpaca_email(mail_subject, mail_content)
    
    done = 'Mail Sent'

    return done