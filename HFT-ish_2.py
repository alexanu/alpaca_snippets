
# Source: https://github.com/parseb/alpaca_martingale_trading_bet/blob/main/alpaca_downbad_martingale.py

import datetime
#import threading
import time
from alpaca_trade_api.rest import TimeFrame

API_KEY = "APIKEY"
API_SECRET = "APIVERYSECRETE"
APCA_API_BASE_URL = "https://paper-api.alpaca.markets" #paper account
APCA_API_DATA_URL= "https://data.alpaca.markets/"

from alpaca_trade_api.rest import REST
import pandas as pd 
import random
#import btalib

alpaca = REST(API_KEY, API_SECRET, APCA_API_BASE_URL)

#%%

BASE_VALUE=int(float(alpaca.get_account().portfolio_value) * 0.01)
MAX_NUM_POSITIONS = 18
TAKE_CUT=  0.003 #0.3%
DOUBLE_D = 0.01
MAX_SAME_POSITION = 3

DAYTRADE_PATTERN_PROTECTION= True
LOSS_AMPLIFIER = 2

nsd= ['NFTY','QSR','INDA','PLUG','QQQ','VEA','IEFA','VXUS','VGT','XLF','XLI','SCHD','VYM','AOA', 
'HON','VNQ','VXX', 'TQQQ' ,'NOW', 'VZ', 'ESTC', 'RBLX', 'WSBF', 'ACGL', 'ACVA', 'ADI', 'ADSK', 
'ALGT', 'ALTR', 'AMD', 'ANGI', 'API', 'ARCC', 'ASML', 'AXON', 'BRKS', 'BSY', 'CDK', 'CDNS', 
'COIN', 'COMM', 'CRSR', 'DBX', 'DDOG', 'DOCU', 'EVBG', 'EWBC', 'EXPI', 'FANG', 'FISV', 'FITB', 
'FORM', 'FUTU', 'GDS', 'GOGL', 'GOOG', 'GOOGL', 'HAS', 'HBAN', 'HOMB', 'HWC', 'IBKR', 'INDB', 
'INTU', 'IPGP', 'JCOM', 'KC', 'KDP', 'KLIC', 'LFUS', 'LITE', 'LPLA', 'LPSN', 'LRCX', 'LYFT', 
'MANH', 'MAT', 'MCFE', 'MCHP', 'MDB', 'MDLZ', 'MKTW', 'MNST', 'MOMO', 'MPWR', 'MRVL', 'MSFT', 
'MTTR', 'MU', 'NCNO', 'NDAQ', 'NICE', 'NSIT', 'NTAP', 'NVDA', 'NXPI', 'OKTA', 'ON', 'OPEN', 'PACW', 
'PAYO', 'PCTY', 'PEGA', 'PLTK', 'POOL', 'POWI', 'PPC', 'PRVA', 'PTC', 'QCOM', 'RCII', 'RMBS', 'RPD', 
'RYAAY', 'SBLK', 'SBNY', 'SEDG', 'SHLS', 'SITM', 'SKYW', 'SMTC', 'SNPS', 'SOFI', 'SPLK', 'SPT', 
'STX', 'SWKS', 'TASK', 'TEAM', 'TFSL', 'TSEM', 'TXN', 'VRNS']

assets= alpaca.list_assets()
assets=pd.DataFrame([asset._raw for asset in assets if asset._raw['fractionable'] and asset._raw['tradable']])

nsdq=[]

for n in nsd:
     if n in list(assets['symbol']):
         nsdq.append(n)

#%%

# RSI > 50 < 75 
#def getRSI(ticker):
    #data= alpaca.get_bars(ticker, TimeFrame.Hour, (datetime.date.today() - datetime.timedelta(days=+2)).isoformat(), datetime.date.today().isoformat(), adjustment='raw').df
    #rsi= btalib.rsi(data).df
    

#%%

def awaitMarketOpen():
    isOpen = alpaca.get_clock().is_open
    while(not isOpen):
      clock = alpaca.get_clock()
      openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
      currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
      timeToOpen = int((openingTime - currTime) / 60)
      print(str(timeToOpen) + " minutes til market open.")
      time.sleep(20)
      isOpen = alpaca.get_clock().is_open
      
      
    return isOpen


#%%

def checkprice(tick):
    return alpaca.get_last_trade(tick).price

def daytrade_sell_check(tick):
    
    today= datetime.date.today().isoformat()
    #treedaysago=(datetime.date.today() - datetime.date.timedelta(days= -3)).isoformat()
    
    activities= alpaca.get_activities(after=today)
    activities= pd.DataFrame([a._raw for a in activities])
    if activities.size > 0:
        not_active_today= tick not in list(activities['symbol'])
    else:
        not_active_today= True
    
    if not DAYTRADE_PATTERN_PROTECTION:
        return True
    elif DAYTRADE_PATTERN_PROTECTION and not_active_today :
        return True
    else:
        return False
        
#%%

def submitorder(tick, b_or_s,positions, num_tick_positions, base_value):
    #state= stock_state[tick]['active']
    #num_tick_positions = stock_state[tick]['positions'] 
    positions= alpaca.list_positions()
    time.sleep(2)
    positions= pd.DataFrame([position._raw for position in positions])
    positions= positions.set_index('symbol').transpose()
    
    
    
    has= list(positions.columns)
    state= tick in has
    if state:
        position= positions[tick]
    
    
    if (state and b_or_s == 'sell') and (float(position.qty) > 0):
        alpaca.submit_order(
        symbol=tick,
        side='sell',
        type='market',
        qty= float(position.qty),
        time_in_force='day'
        )
        
    if state and b_or_s == 'buy' and num_tick_positions < MAX_SAME_POSITION:
        print('*******', tick, state, position.cost_basis)
        print("Attempting Double Down---DD------ Ticker: " + tick)
        #stock_state[tick]['last_spent'] = stock_state[tick]['last_spent'] * 2
        howmuch = float(position.cost_basis) * LOSS_AMPLIFIER
        alpaca.submit_order(
            symbol= tick,
            side='buy',
            type='market',
            notional=str(howmuch),
            time_in_force='day'
            )

    if not state and b_or_s == 'buy':
        alpaca.submit_order(
            symbol= tick,
            side='buy',
            type='market',
            notional=str(base_value),
            time_in_force='day'
            )


#%%

## 2**k-1


    
    

#%%


while True:
    awaitMarketOpen()
    account= alpaca.get_account()
    BASE_VALUE=float(account.portfolio_value) * 0.01
    positions= alpaca.list_positions()
    orders= alpaca.list_orders()
    numpositions= len(positions)
    numorders= len(orders)
    
    orders = pd.DataFrame([order._raw for order in orders])
    positions= pd.DataFrame([position._raw for position in positions])
    
    
    if  not positions.empty:
        positions=positions.set_index('symbol').transpose()
    
    
    p_sym=list(positions.columns)
        
    
    print('#####-----#####')
    print(positions)
    print("numpositions: " + str(numpositions) + " / "  + str(MAX_NUM_POSITIONS) )
    

    for p in p_sym:
        #sell_allowed= daytrade_sell_check(p)
        account= alpaca.get_account()
        sell_allowed = account.daytrade_count < 3
        price= checkprice(p)
        position= positions[p]
        sell_at= float(position.avg_entry_price) * TAKE_CUT +  float(position.avg_entry_price) 
        orders_count= round(float(position.cost_basis)) / round(BASE_VALUE)
        daytrade= daytrade_sell_check(p)
        
        print('*******', p, price, sell_at, position.cost_basis)
        time.sleep(1)
        
        if (price > sell_at) and sell_allowed and daytrade:
            submitorder(p,'sell', positions, orders_count, BASE_VALUE)
            
        elif (p not in orders) and (price < sell_at - (sell_at * (DOUBLE_D * orders_count) * 1.8)):
            submitorder(p,'sell', positions, orders_count, BASE_VALUE)
            nsdq.remove(p)
    
        elif (p not in orders)  and (price < (sell_at - (sell_at * (DOUBLE_D * orders_count)))): 
            submitorder(p,'buy',positions, orders_count, BASE_VALUE)
            

    time.sleep(10)
    #print(positions)
    print('-----$$$$$$ '+ ' CASH: ' + alpaca.get_account().cash + '--- PORTFOLIO_VALUE: ' + alpaca.get_account().portfolio_value  + ' $$$$$$-----')
    time.sleep(45)
    
    if numpositions < MAX_NUM_POSITIONS:
        
        randtick = random.choice( list( set(nsdq) - set(p_sym) ) )
        num_positions=0
        submitorder(randtick,'buy',positions,num_positions, BASE_VALUE)

        price=checkprice(randtick)
        
        print('Bought -- ' + randtick +' at: ' + str(price) )
        time.sleep(5)