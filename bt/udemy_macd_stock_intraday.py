
import datetime as dt
import pandas as pd
from copy import deepcopy
import numpy as np

import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit
from Alpaca_config import *
from bt.bt_utils import *
from Universes import Famous
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
tickers = Famous


days = 200
minute_frame = 10 # means 1 day is 39 rows => 43k rows for 5 years 
today = dt.datetime.now().strftime("%Y-%m-%d")
n_days_ago = (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%d")
historicalData = {}
for symbol in tickers:
    temp = alpaca.get_bars(symbol, TimeFrame(minute_frame, TimeFrameUnit.Minute), n_days_ago, today,adjustment='raw').df
    temp.between_time('09:31', '16:00') # focus on market hours as for now trading on alpaca is restricted to market hours
    temp.index = temp.index.tz_localize(None) # remove +00:00 from datetime
    historicalData[symbol]=temp
    
#####################################   BACKTESTING   ##################################
ohlc_dict = deepcopy(historicalData)
stoch_signal = {}
tickers_signal = {}
tickers_ret = {}
trade_count = {}
trade_data = {}
hwm = {}

# some new columns will be added to every dataframe
stochastic(ohlc_dict)
MACD(ohlc_dict)

for ticker in tickers:
    print('Initiating dictionaries')
    ohlc_dict[ticker].dropna(inplace=True) # we do drop NA as for some lines it was not enough history to calculate indicators
    stoch_signal[ticker] = "" # placeholder for "overbought" or "oversold", but for long-only we need just "oversold"
    trade_count[ticker] = 0 # number of trades
    tickers_signal[ticker] = "" # will be "buy" as this is long-only strat
    hwm[ticker] = 0 # high water mark for trailing stop
    tickers_ret[ticker] = [0]
    trade_data[ticker] = {} # statistic for trade analysis
    
for ticker in tickers:
    print("Calculating daily returns for ",ticker)
    for i in range(1,len(ohlc_dict[ticker])-1): # here we don't do for 1st and last row as we are usin [i-1] and [i+1] in code
        if ohlc_dict[ticker]["%K"][i] < 20:
            stoch_signal[ticker] = "oversold"
        elif ohlc_dict[ticker]["%K"][i] > 80:
            stoch_signal[ticker] = "overbought"
        
        if tickers_signal[ticker] == "":
            tickers_ret[ticker].append(0) # to have the same num of rows as ohlc dict for future merger
            if ohlc_dict[ticker]["macd"][i]> ohlc_dict[ticker]["signal"][i] and \
               ohlc_dict[ticker]["macd"][i-1]< ohlc_dict[ticker]["signal"][i-1] and \
               stoch_signal[ticker]=="oversold":
                   tickers_signal[ticker] = "Buy" # all conditions are satisfied => "buy"
                   trade_count[ticker]+=1
                   trade_data[ticker][trade_count[ticker]] = [ohlc_dict[ticker]["open"][i+1]]
                   hwm[ticker] = ohlc_dict[ticker]["open"][i+1] # we will manage to enter the trade only on next bar:
                                                                       # we take here next bar open, 
                                                                       # but to improve logic, probably, 
                                                                       # we could take some open+high combination
                     
        elif tickers_signal[ticker] == "Buy":
            if ohlc_dict[ticker]["low"][i]<0.985*hwm[ticker]: # stop is fired up
                tickers_signal[ticker] = "" # clear the dic from buying
                trade_data[ticker][trade_count[ticker]].append(0.985*hwm[ticker]) # sell price
                trade_count[ticker]+=1
                tickers_ret[ticker].append((0.985*hwm[ticker]/ohlc_dict[ticker]["close"][i-1])-1)
            else:
                hwm[ticker] = max(hwm[ticker],ohlc_dict[ticker]["high"][i]) # updating stop-loss when price went up
                tickers_ret[ticker].append((ohlc_dict[ticker]["close"][i]/ohlc_dict[ticker]["close"][i-1])-1) # return of every bar
                            
    if trade_count[ticker]%2 != 0: # if not-even number of trades: after end of history a trade could be still open ...
        trade_data[ticker][trade_count[ticker]].append(ohlc_dict[ticker]["close"][i+1]) # ... make artificial exit
    
    tickers_ret[ticker].append(0) #since we are removing the last row
    ohlc_dict[ticker]["ret"] = np.array(tickers_ret[ticker]) # add return column to the df

# calculating overall strategy's KPIs
trade_df = {}
overall_return = 0
for ticker in tickers:
    trade_df[ticker] = pd.DataFrame(trade_data[ticker]).T
    trade_df[ticker].columns = ["trade_entry_pr","trade_exit_pr"] # rename columns
    trade_df[ticker]["return"] = trade_df[ticker]["trade_exit_pr"]/trade_df[ticker]["trade_entry_pr"] # trade return
                    # important that the name of column is "return" as it is used by formulas for performance
    print("total return {} = {}".format(ticker,trade_df[ticker]["return"].cumprod().iloc[-1] - 1)) # total return for ticker
    overall_return+= (1/len(tickers.split(",")))*(trade_df[ticker]["return"].cumprod().iloc[-1] - 1)
                    # 1/n because we invest equally pro ticker
print("Overall Return of Strategy = {}".format(overall_return))
  
#calculating individual stock's KPIs
win_rate = {}
mean_ret_pt = {}
mean_ret_pwt = {}
mean_ret_plt = {}
max_cons_loss = {}
for ticker in tickers.split(","):
    print("calculating intraday KPIs for ",ticker)
    win_rate[ticker] =  winRate(trade_df[ticker])      
    mean_ret_pt[ticker] =  meanretpertrade(trade_df[ticker])
    mean_ret_pwt[ticker] =  meanretwintrade(trade_df[ticker])
    mean_ret_plt[ticker] =  meanretlostrade(trade_df[ticker])
    max_cons_loss[ticker] =  maxconsectvloss(trade_df[ticker])

KPI_df = pd.DataFrame([win_rate,mean_ret_pt,mean_ret_pwt,mean_ret_plt,max_cons_loss],
                      index=["Win Rate","Mean Return Per Trade","MR Per WR", "MR Per LR", "Max Cons Loss"])      
KPI_df.T
    