
# Source: https://github.com/parseb/alpaca_martingale_trading_bet/blob/main/alpaca_downbad_martingale.py

import pandas as pd 
import datetime
import time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
from Alpaca_config import *
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')

import utils_for_alpaca

MAX_NUM_POSITIONS = 18
TAKE_CUT=  0.003 #0.3%
DOUBLE_D = 0.01
MAX_SAME_POSITION = 3

LOSS_AMPLIFIER = 2


def submitorder(tick, b_or_s,positions, num_tick_positions, base_value):
    positions= alpaca.list_positions()
    time.sleep(2)
    positions= pd.DataFrame([position._raw for position in positions])
    positions= positions.set_index('symbol').transpose()
    
    has= list(positions.columns)
    state= tick in has
    if state:
        position= positions[tick]
    
    if (state and b_or_s == 'sell') and (float(position.qty) > 0):
        alpaca.submit_order(symbol=tick,side='sell',type='market',qty= float(position.qty),time_in_force='day')
        
    if state and b_or_s == 'buy' and num_tick_positions < MAX_SAME_POSITION:
        howmuch = float(position.cost_basis) * LOSS_AMPLIFIER
        alpaca.submit_order(symbol= tick,side='buy',type='market',notional=str(howmuch),time_in_force='day')

    if not state and b_or_s == 'buy':
        alpaca.submit_order(symbol= tick,side='buy',type='market',notional=str(base_value),time_in_force='day')

while True:
    account= alpaca.get_account()
    BASE_VALUE=int(float(account.portfolio_value) * 0.01)
    positions= alpaca.list_positions()
    orders= alpaca.list_orders()
    numpositions= len(positions)
    numorders= len(orders)
    
    orders = pd.DataFrame([order._raw for order in orders])
    positions= pd.DataFrame([position._raw for position in positions])
    
    if  not positions.empty:
        positions=positions.set_index('symbol').transpose()
    
    for p in list(positions.columns):
        #sell_allowed= daytrade_sell_check(p)
        account= alpaca.get_account()
        sell_allowed = account.daytrade_count < 3
        price= alpaca.get_last_trade(p).price
        position= positions[p]
        sell_at= float(position.avg_entry_price) * (1 + TAKE_CUT)
        orders_count= round(float(position.cost_basis)) / round(BASE_VALUE)
        daytrade= utils_for_alpaca.daytrade_sell_check(p)
        
        time.sleep(1)
        
        if (price > sell_at) and sell_allowed and daytrade:
            submitorder(p,'sell', positions, orders_count, BASE_VALUE)
            
        elif (p not in orders) and (price < sell_at - (sell_at * (DOUBLE_D * orders_count) * 1.8)):
            submitorder(p,'sell', positions, orders_count, BASE_VALUE)
            nsdq.remove(p)
    
        elif (p not in orders)  and (price < (sell_at - (sell_at * (DOUBLE_D * orders_count)))): 
            submitorder(p,'buy',positions, orders_count, BASE_VALUE)
            
    time.sleep(45)
