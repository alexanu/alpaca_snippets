
# This strat has a backtest counterpart: "udemy_macd_stoh_intraday.py" in bt folder
import datetime as dt
import time

import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit
from Alpaca_config import *
from bt.bt_utils import *
from Universes import Famous
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
tickers = Famous

max_pos = 3000 #max position size for each ticker

stoch_signal = {}
for ticker in tickers:
    stoch_signal[ticker] = ""


def main():
    global stoch_signal

    days = 3
    minute_frame = 1
    today = dt.datetime.now().strftime("%Y-%m-%d")
    n_days_ago = (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    historicalData = {}
    for symbol in tickers:
        temp = alpaca.get_bars(symbol, TimeFrame(minute_frame, TimeFrameUnit.Minute), n_days_ago, today,adjustment='raw').df
        temp.between_time('09:31', '16:00') # focus on market hours as for now trading on alpaca is restricted to market hours
        temp.index = temp.index.tz_localize(None) # remove +00:00 from datetime
        historicalData[symbol]=temp

    # some new columns will be added to every dataframe
    MACD(historicalData)
    stochastic(historicalData)

    positions = alpaca.list_positions()
    time.sleep(2) # wait for execution of an order
    
    for ticker in tickers:
        historicalData[ticker].dropna(inplace=True) # we do drop NA as for some lines it was not enough history to calculate indicators
        existing_pos = False
        
        if historicalData[ticker]["%K"][-1] < 20:
            stoch_signal[ticker] = "oversold"
        elif historicalData[ticker]["%K"][-1] > 80:
            stoch_signal[ticker] = "overbought"
        
        for position in positions:
            if len(positions) > 0:
                if position.symbol == ticker and position.qty !=0: # we already long this ticker
                    print("existing position of {} stocks in {}...skipping".format(position.qty, ticker))
                    existing_pos = True
        
        if historicalData[ticker]["macd"].iloc[-1]> historicalData[ticker]["signal"].iloc[-1] and \
            historicalData[ticker]["macd"].iloc[-2]< historicalData[ticker]["signal"].iloc[-2] and \
            stoch_signal[ticker]=="oversold" and existing_pos == False:
            # .iloc[-1] means last row
                alpaca.submit_order(ticker, max(1,int(max_pos/historicalData[ticker]["close"].iloc[-1])), "buy", "market", "ioc")
                print("bought {} stocks in {}".format(int(max_pos/historicalData[ticker]["close"].iloc[-1]),ticker))
                time.sleep(2) # wait for execution of an order
                try:
                    filled_qty = alpaca.get_position(ticker).qty # comes as string
                    time.sleep(2) # wait for execution of an api request
                    alpaca.submit_order(ticker, int(filled_qty), "sell", "trailing_stop", "day", trail_percent = "1.5")
                except Exception as e:
                    print(ticker, e)


starttime = time.time()
timeout = starttime + 60*60*1 # will run for 1 hour
while time.time() <= timeout:
    print("starting iteration at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
    main()
    time.sleep(60 - ((time.time() - starttime) % 60)) # sleep for the rest of current minute

# close out all positions and orders    
alpaca.close_all_positions() # wait for execution of sell orders
time.sleep(5)
alpaca.cancel_all_orders()
time.sleep(5)