# https://alpaca.markets/docs/api-documentation/api-v2/market-data/streaming/

import alpaca_trade_api

from alpaca_trade_api.stream import Stream
from Alpaca_config import *

alpaca_trade_api.__version__

import logging
log = logging.getLogger(__name__)


async def print_trade(t):
    print('trade', t)

async def print_quote(q):
    print('quote', q)

async def print_trade_update(tu):
    print('trade update', tu)

async def print_crypto_trade(t):
    print('crypto trade', t)

def main():
    logging.basicConfig(level=logging.INFO)
    feed = 'SIP'
    stream = Stream(API_KEY_PAPER, API_SECRET_PAPER, data_feed=feed, raw_data=True)
    stream.subscribe_trade_updates(print_trade_update)
    stream.subscribe_trades(print_trade, 'AAPL')
    stream.subscribe_quotes(print_quote, 'IBM')
    stream.subscribe_crypto_trades(print_crypto_trade, 'BTCUSD')

    @stream.on_bar('MSFT')
    async def _(bar):
        print('bar', bar)

    @stream.on_status("*")
    async def _(status):
        print('status', status)

    @stream.on_luld('AAPL', 'MSFT')
    async def _(luld):
        print('LULD', luld)

    stream.run()

if __name__ == "__main__":
    main()



# ----------------------------------------------------------------------------------------------------





import alpaca_trade_api as tradeapi
import threading
import time



# instantiate REST API
api = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, api_version='v2')

# init WebSocket - How do I use WebSockets to stream data with the Alpaca API?
'''
# Under the hood the StreamConn object is using the websockets python module which is based on asyncio
    It has an event loop that schedules asyncio tasks. And instead of waiting for outside entities, the context is moved between tasks.
    When a quote is received from the servers, an asyncio task we've created is called to print the received quote. That is the on_quotes method. So that means we could create callbacks to let our code decide what to do with the data.
    And most important - when we call conn.run() it blocks. That means that we cannot write code after that point because it will not execute while the websocket connection is running. You may ask yourself "how to stop the connection/change the subscription?". Keep reading.
    
# websockets pythom module: https://websockets.readthedocs.io/en/stable/intro.html

# Alpaca only supports one websocket connection at a time. See word doc to overcome this

'''

#------------------------------------------------------------------------------------------
# create a StreamConn object that subscribes to google (GOOG) quotes and prints them once received

if __name__ == '__main__':
    conn = StreamConn(
        API_KEY_PAPER,
        API_SECRET_PAPER,
        base_url=URL('https://paper-api.alpaca.markets'),
        data_url=URL('https://data.alpaca.markets'),
        data_stream='alpacadatav1'
    )

    @conn.on(r'Q\..+')
    async def on_quotes(conn, channel, quote):
        print('quote', quote)
        
    conn.run(['alpacadatav1/Q.GOOG'])

#------------------------------------------------------------------------------------------




conn = tradeapi.stream2.StreamConn(API_KEY_PAPER, API_SECRET_PAPER, base_url=API_BASE_URL_PAPER, data_url=API_DATA_URL, data_stream='alpacadatav1')



client_order_id = r'my_client_order_id'
@conn.on(client_order_id)
async def on_msg(conn, channel, data):
    # Print the update to the console.
    print("Update for {}. Event: {}.".format(client_order_id, data['event']))

conn.run(['trade_updates'])


import json
trade_msg = []
@conn.on(r'^trade_updates$')
async def on_trade_updates(conn, channel, trade):
    print('trade', trade)
    trade_msg.append(trade)
    if 'fill' in trade.event:
        past_trades.append([trade.order['updated_at'],trade.order['symbol'],trade.order['side'],trade.order['filled_qty'],trade.order['filled_avg_price']])
        with open('past_trades.csv', 'w') as f:
            json.dump(past_trades, f, indent=4)
        print(past_trades[-1])



order_msg = []
@conn.on(r'^account_updates$')
async def on_account_updates(conn, channel, account):
    print('account', account)
    order_msg.append(account)


@conn.on(r'^status$')
async def on_status(conn, channel, data):
    print('status update', data)


@conn.on(r'^T.AAPL$')
async def trade_info(conn, channel, bar):
    print('bars', bar)
    print(bar._raw)


@conn.on(r'^Q.AAPL$')
async def quote_info(conn, channel, bar):
    print('bars', bar)


@conn.on(r'^AM.AAPL$')
async def on_minute_bars(conn, channel, bar):
    print('bars', bar)



# start websocket
def ws_start():
    conn.run(['account_updates', 'trade_updates', 'AM.AAPL', 'Q.AAPL', 'T.AAPL']) # Start listening for updates



ws_thread = threading.Thread(target=ws_start, daemon=True)
ws_thread.start()


# Let the websocket run for 30 seconds
time.sleep(30)


list_of_price_queues = []

@conn.on(r'^AM$')
async def on_minute_bars(conn, channel, data):
    global list_of_price_queues
    print('Data: ', data.close)
    [q.append(data) for q in list_of_price_queues]


class Data_Stream():
    def __init__(self, symbol):
        self.symbol = 'AM.*'
        #self.symbol = 'AM.%s' %(symbol)
        global list_of_price_queues
        conn.run([self.symbol])

    def get_stream_data(self):
        return list_of_price_queues



#----------------------------------------------------------------------------------------------------


async def trade_callback(t):
    print('trade', t)

async def quote_callback(q):
    print('quote', q)


# Initiate Class Instance
stream = Stream(<ALPACA_API_KEY>,
                <ALPACA_SECRET_KEY>,
                base_url=URL('https://paper-api.alpaca.markets'),
                data_feed='SIP')  # <- replace to SIP if you have PRO subscription

# subscribing to event
stream.subscribe_trades(trade_callback, 'AAPL')
stream.subscribe_quotes(quote_callback, 'IBM')

stream.run()


# --------------------------------------------------------------------------------------------


import json
import alpaca_trade_api as tradeapi

base_url = 'https://paper-api.alpaca.markets'

with open("keys.json", "r") as f:
    key_dict = json.loads(f.readline().strip())

#api = tradeapi.REST(key_dict['api_key_id'], key_dict['api_secret'], base_url, api_version='v2')
#print(api.get_account())

list_of_price_queues = []

conn = tradeapi.StreamConn(
        base_url=base_url,
        #data_stream='polygon',
        key_id=key_dict['api_key_id'],
        secret_key=key_dict['api_secret']
    )

@conn.on(r'^account_updates$')
async def on_account_updates(conn, channel, account):
    print('account', account)

@conn.on(r'^AM$')
async def on_minute_bars(conn, channel, data):
    global list_of_price_queues
    print('Data: ', data.close)
    [q.append(data) for q in list_of_price_queues]

class Data_Stream():
    def __init__(self, symbol):
        self.symbol = 'AM.*'
        #self.symbol = 'AM.%s' %(symbol)
        global list_of_price_queues
        conn.run([self.symbol])

    def get_stream_data(self):
        return list_of_price_queues

#-----------------------------------------------------------------
stream.subscribe_bars(barReceived, "AAPL", "MSFT", "TSLA")

listOfSymbols = ["AAPL", "MSFT", "TSLA"]
stream.subscribe_bars(barReceived, *listOfSymbols)

# if your tickers are stored in a list called tickers, you can "splat" that list by calling like this
# if you call it without the * in front of tickers, then python will understand that as being one argument that is a list rather than multiple arguments that get collected into a tuple
self.stream.subscribe_quotes(default_quote, *tickers)


# --------------------------------------------------------------
# No SDK stream data (from Udemy)

    import Alpaca_config
    import websocket, json

    endpoint_stream = "wss://stream.data.alpaca.markets/v2/sip" # I have a paid subscription
    headers = json.loads(open("Alpaca_config.txt",'r').read())

    trades_stream = ["AAPL"]
    quotes_stream = ["AMD","CLDR"]
    bars_stream = ["AAPL","VOO"]

    def on_open(ws):
        auth = {
                "action": "auth",
                "key": headers["APCA-API-KEY-ID"],
                "secret": headers["APCA-API-SECRET-KEY"]
            }
        
        ws.send(json.dumps(auth)) # json.dumps convert json to string
        
        message = {
                    "action": "subscribe",
                    "trades": trades_stream,
                    "quotes": quotes_stream,
                    "bars": bars_stream
                }
                    
        ws.send(json.dumps(message))
    
    def on_message(ws, message):
        print(message)

    def on_close(ws):
        print("closed connection")

    ws = websocket.WebSocketApp(endpoint_stream, on_open=on_open, on_message=on_message, on_close=on_close)
    ws.run_forever()


# No SDK, old websocket - update is needed

    import websocket, json, requests, sys
    from config import *
    import dateutil.parser
    from datetime import datetime

    minutes_processed = {}
    minute_candlesticks = []
    current_tick = None
    previous_tick = None
    in_position = False

    BASE_URL = "https://paper-api.alpaca.markets"
    ACCOUNT_URL = "{}/v2/account".format(BASE_URL)
    ORDERS_URL = "{}/v2/orders".format(BASE_URL)
    POSITIONS_URL = "{}/v2/positions/{}".format(BASE_URL, SYMBOL)

    HEADERS = {'APCA-API-KEY-ID': API_KEY, 'APCA-API-SECRET-KEY': SECRET_KEY}

    def place_order(profit_price, loss_price):
        data = {
            "symbol": SYMBOL,
            "qty": 1,
            "side": "buy",
            "type": "market",
            "time_in_force": "gtc",
            "order_class": "bracket",
            "take_profit": {
                "limit_price": profit_price
            },
            "stop_loss": {
                "stop_price": loss_price
            }
        }

        r = requests.post(ORDERS_URL, json=data, headers=HEADERS)

        response = json.loads(r.content)

        print(response)

    def on_open(ws):
        print("opened")
        auth_data = {
            "action": "auth",
            "params": API_KEY
        }

        ws.send(json.dumps(auth_data))

        channel_data = {
            "action": "subscribe",
            "params": TICKERS
        }

        ws.send(json.dumps(channel_data))


    def on_message(ws, message):
        global current_tick, previous_tick, in_position

        previous_tick = current_tick
        current_tick = json.loads(message)[0]

        print(current_tick)
        print("=== Received Tick ===")
        print("{} @ {}".format(current_tick['t'], current_tick['bp']))
        tick_datetime_object = datetime.utcfromtimestamp(current_tick['t']/1000)
        tick_dt = tick_datetime_object.strftime('%Y-%m-%d %H:%M')

        print(tick_datetime_object.minute)
        print(tick_dt)

        if not tick_dt in minutes_processed:
            print("starting new candlestick")
            minutes_processed[tick_dt] = True
            print(minutes_processed)
        
            if len(minute_candlesticks) > 0:
                minute_candlesticks[-1]['close'] = previous_tick['bp']

            minute_candlesticks.append({
                "minute": tick_dt,
                "open": current_tick['bp'],
                "high": current_tick['bp'],
                "low": current_tick['bp']
            })
            

        if len(minute_candlesticks) > 0:
            current_candlestick = minute_candlesticks[-1]
            if current_tick['bp'] > current_candlestick['high']:
                current_candlestick['high'] = current_tick['bp']
            if current_tick['bp'] < current_candlestick['low']:
                current_candlestick['low'] = current_tick['bp']

        print("== Candlesticks ==")
        for candlestick in minute_candlesticks:
            print(candlestick)

        if len(minute_candlesticks) > 3:
            print("== there are more than 3 candlesticks, checking for pattern ==")
            last_candle = minute_candlesticks[-2]
            previous_candle = minute_candlesticks[-3]
            first_candle = minute_candlesticks[-4]

            print("== let's compare the last 3 candle closes ==")
            if last_candle['close'] > previous_candle['close'] and previous_candle['close'] > first_candle['close']:
                print("=== Three green candlesticks in a row, let's make a trade! ===")
                distance = last_candle['close'] - first_candle['open']
                print("Distance is {}".format(distance))
                profit_price = last_candle['close'] + (distance * 2)
                print("I will take profit at {}".format(profit_price))
                loss_price = first_candle['open']
                print("I will sell for a loss at {}".format(loss_price))

                if not in_position:
                    print("== Placing order and setting in position to true ==")
                    in_position = True
                    place_order(profit_price, loss_price)
                    sys.exit()
            else:
                print("No go")


    def on_close(ws):
        print("closed connection")

    socket = "wss://alpaca.socket.polygon.io/stocks"

    ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
    ws.run_forever()


# No SDK, no Alpaca, but could re-use

    import cbpro
    import os
    import websocket
    import pprint
    import ta
    import numpy as np
    import pandas as pd
    import cbpro, time
    public_client = cbpro.PublicClient()

    #coinbase setup
    trade_symbol = "ETH-USD"
    traded_quantity = .05
    cbpro_fee = .005

    #rsi setup
    rsi_period = 14
    rsi_overbought = 70
    rsi_oversold = 30

    #data storage setup
    prices = []
    open_position = False
    total_buys = 0
    total_sells = 0
    order_book = {}
    order_book['rsiBUY'] = []
    order_book['priceBUY'] = []
    order_book['priceSELL'] = []
    order_book['rsiSELL'] = []
    order_book['quantityPriceBUY'] = []
    order_book['quantityPriceSELL'] = []
    order_book['feeBUY'] = []
    order_book['feeSELL'] = []

    #inherit from the cbpro websocket client
    class myWebsocketClient(cbpro.WebsocketClient):
        
        #what to do when we first initially start the client
        def on_open(self):
            self.url = "wss://ws-feed.pro.coinbase.com/"
            self.products = trade_symbol
            self.channels = ["ticker"] #ticker, heartbeat, level2
            print(f"{trade_symbol} RSI Trading Bot has begun!")
            
        #what to do for each row of data that is reserved to our client from the websocket source (server)    
        def on_message(self, msg):
            global prices, open_position, total_buys, total_sells
            try:
                #start trying to gather up the price data into an array.
                prices.append(float(msg['price']))
                #data interval (5min)
                #time.sleep(300)

            except:
                pass

            #Get enough data in our prices array to then start calculating RSI and trading on that calculation
            if len(prices) > rsi_period:
                np_prices = np.array(prices)
                ser = pd.Series(np_prices)
                rsi = ta.momentum.rsi(ser, rsi_period, False)
                last_rsi = rsi.iloc[-1]
                #print("Current Price: ", float(msg['price'])," | Current RSI: {}".format(last_rsi))

                #overbought trading order (sell high)
                if last_rsi > rsi_overbought:
                    if open_position:
                        print("Overbought! SELL!")
                        order_book['rsiSELL'].append(last_rsi)
                        order_book['priceSELL'].append(float(msg['price']))
                        order_book['quantityPriceSELL'].append(float(msg['price'])*traded_quantity)
                        order_book['feeSELL'].append(float(msg['price'])*traded_quantity*cbpro_fee)
                        open_position = False
                        total_sells+=1
                        print("Number of Sells: ", total_sells, " at an RSI of ", last_rsi)
                    else:
                        pass
                        #print("You don't own anything.")

                #oversold trading order (buy low)
                if last_rsi < rsi_oversold:
                    if open_position:
                        pass
                        #print("You already own it.")
                    else:
                        print("Oversold! BUY!")
                        order_book['rsiBUY'].append(last_rsi)
                        order_book['priceBUY'].append(float(msg['price']))
                        order_book['quantityPriceBUY'].append(float(msg['price'])*traded_quantity)
                        order_book['feeBUY'].append(float(msg['price'])*traded_quantity*cbpro_fee)
                        open_position = True
                        total_buys+=1
                        print("Number of Buys: ", total_buys, " at an RSI of ", last_rsi)
                        
        #lets grab our results when we decide to close our websocket connection
        def on_close(self):
            print("------------ Results ------------\n")
            results = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in order_book.items() ]))
            results['returnsBeforeFees'] = results.quantityPriceSELL - results.quantityPriceBUY
            results['returnsAfterFees'] = results.quantityPriceSELL - results.quantityPriceBUY - results.feeBUY - results.feeSELL
            display(results)
            print('\n Total returns before Fees: ',results.returnsBeforeFees.sum())
            print('\n Total returns after Fees: ', results.returnsAfterFees.sum())
            #print(prices)

    wsClient = myWebsocketClient()
    wsClient.start()