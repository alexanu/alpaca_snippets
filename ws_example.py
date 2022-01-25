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


##--------------------------------------------------------------------------------------------------------
# From Udemy: not using Alpaca SDK, but directly via websocket

import Alpaca_config
import websocket, json

def on_open(ws):
    print("opened")
    auth_data = {
        "action": "authenticate",
        "data": {"key_id": API_KEY_PAPER, "secret_key": API_SECRET_PAPER}
    }

    ws.send(json.dumps(auth_data))
    listen_message = {"action": "listen", "data": {"streams": ["AM.TSLA"]}}
    ws.send(json.dumps(listen_message))

def on_message(ws, message):
    print("received a message")
    print(message)

def on_close(ws):
    print("closed connection")

socket = "wss://data.alpaca.markets/stream"

ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
ws.run_forever()

# -------------------------------------------------------------------------------------------------

import alpaca_trade_api as tradeapi
conn = tradeapi.stream2.StreamConn()
@conn.on(r'^trade_updates$')
async def on_trade_updates(conn, channel, trade):
    order = trade.order

    broker = Broker.get_instance()
    symbol = order['symbol']

    if trade.event == 'fill':
        if order["side"] == 'buy':
            broker.set_filled(symbol)
        if order['side'] == 'sell':
            # Update our positions
            broker.set_unfilled(symbol)

    elif trade.event == 'canceled':
        broker.cancel_order(symbol)

#-----------------------------------------------------------------
stream.subscribe_bars(barReceived, "AAPL", "MSFT", "TSLA")

listOfSymbols = ["AAPL", "MSFT", "TSLA"]
stream.subscribe_bars(barReceived, *listOfSymbols)

# if your tickers are stored in a list called tickers, you can "splat" that list by calling like this
# if you call it without the * in front of tickers, then python will understand that as being one argument that is a list rather than multiple arguments that get collected into a tuple
self.stream.subscribe_quotes(default_quote, *tickers)


# --------------------------------------------------------------
# Stream with websocket without SDK (from Udemy)

import websocket
import json

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

ws = websocket.WebSocketApp(endpoint_stream, on_open=on_open, on_message=on_message)
ws.run_forever()
