# Finding top movers using streaming
# from Udemy, so doesn't use SDK

import websocket
import json
import threading
import time

from utils_for_alpaca import *
from Universes import *

import alpaca_trade_api as tradeapi
from Alpaca_config import *
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
endpoint_stream = "wss://stream.data.alpaca.markets/v2/sip" # I have a paid subscription
headers = json.loads(open("Alpaca_config.txt",'r').read())


tickers = Universe_SP100
df = get_tkrs_snapshot(alpaca,tickers)
tickers = df.index.tolist() # to avoid that some tickers are not available
ltp = {} #dictionary to store ltp information for each ticker
prev_close = {} #dictionary to store previous day's close price information for each ticker 
perc_change = {} #dictionary to store percentage change from yesterday's close for each ticker
traded_tickers = [] #storing tickers which have been traded and therefore to be excluded
max_pos = 3000 #max position size for each ticker
for ticker in tickers:
    prev_close[ticker] = df["prev_close"][ticker]
    ltp[ticker] = df["price"][ticker]
    perc_change[ticker] = 0


def on_open(ws):
    auth = {
            "action": "auth",
            "key": headers["APCA-API-KEY-ID"],
            "secret": headers["APCA-API-SECRET-KEY"]
           }
    ws.send(json.dumps(auth)) # json.dumps convert json to string
    message = {
                "action": "subscribe",
                "quotes": tickers, # maybe not quotes, but trades
              }               
    ws.send(json.dumps(message))

def on_message(ws, message):
    #print(message)
    tick = json.loads(message)
    tkr = tick["stream"].split(".")[-1] # tick[0]["S"]
    ltp[tkr] = float(tick["data"]["p"]) # last traded price
    perc_change[tkr] = round((ltp[tkr]/prev_close[tkr] - 1)*100,2)   


def connect():
    ws = websocket.WebSocketApp(endpoint_stream, on_open=on_open, on_message=on_message)
    ws.run_forever()


def pos_size(ticker):
    return max(1,int(max_pos/ltp[ticker]))

def signal(traded_tickers):
    #print(traded_tickers)
    for ticker, pc in perc_change.items(): # despite perc_change is coming from another thread, we able to use it
        #   (ticker, pc)
        if pc > 2 and ticker not in traded_tickers:
            alpaca.submit_order(ticker, pos_size(ticker), "buy", "market", "ioc")
            time.sleep(3) # we need pause to giv time to execute
            try: # we take this in "try" to avoid situation when order was not filled
                filled_qty = alpaca.get_position(ticker).qty
                time.sleep(1) # we need pause to giv time to execute
                alpaca.submit_order(ticker, int(filled_qty), "sell", "trailing_stop", "day", trail_percent = "1.5")
                traded_tickers.append(ticker)
            except Exception as e:
                print(ticker, e)
        if pc < -2 and ticker not in traded_tickers:
            alpaca.submit_order(ticker, pos_size(ticker), "sell", "market", "ioc")
            time.sleep(3) # we need pause to giv time to execute
            try:
                filled_qty = alpaca.get_position(ticker).qty
                time.sleep(1) # we need pause to giv time to execute
                alpaca.submit_order(ticker, -1*int(filled_qty), "buy", "trailing_stop", "day", trail_percent = "1.5")
                traded_tickers.append(ticker)
            except Exception as e:
                print(ticker, e)


con_thread = threading.Thread(target=connect, daemon=True) # we need to send steaming to another thread
con_thread.start()

starttime = time.time()
timeout = starttime + 60*60 # 1 hour
while time.time() <= timeout:
    for ticker in tickers:
        print("percent change for {} = {}".format(ticker,perc_change[ticker])) # despite perc_change is coming from another thread, we able to use it
        signal(traded_tickers)
    time.sleep(60 - ((time.time() - starttime) % 60))

#closing all positions and cancelling all orders at the end of the strategy  
alpaca.close_all_positions()
alpaca.cancel_all_orders()
time.sleep(5)