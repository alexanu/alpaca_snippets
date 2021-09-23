"""
In this example code we will show a pattern that allows a user to change
the websocket subscriptions as they please.

conn.run() method is blocking, meaning we cannot execute anything after that. 
We need to use another thread to achieve that we call the subscribe method that will change the subscription

Change the subscription of the websocket without restarting it. 
    - For instance if we want to subscribe to google quotes and then want to change it to apple quotes.

"""



import logging
import threading
import asyncio
import time
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL

ALPACA_API_KEY = "<YOUR-API-KEY>"
ALPACA_SECRET_KEY = "<YOUR-SECRET-KEY>"
USE_POLYGON = False


async def print_trade(t):
    print('trade', t)


async def print_quote(q):
    print('quote', q)


async def print_bar(bar):
    print('bar', bar)

PREVIOUS = None


def consumer_thread():
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    global conn
    conn = Stream(ALPACA_API_KEY,
                  ALPACA_SECRET_KEY,
                  base_url=URL('https://paper-api.alpaca.markets'),
                  data_feed='iex')

    conn.subscribe_quotes(print_quote, 'AAPL')
    global PREVIOUS
    PREVIOUS = "AAPL"
    conn.run()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    threading.Thread(target=consumer_thread).start()

    loop = asyncio.get_event_loop()

    time.sleep(5)  # give the initial connection time to be established
    subscriptions = {"BABA": print_quote,
                     "AAPL": print_quote,
                     "TSLA": print_quote,
                     }

    while 1:
        for ticker, handler in subscriptions.items():
            conn.unsubscribe_quotes(PREVIOUS)
            conn.subscribe_quotes(handler, ticker)
            PREVIOUS = ticker
            time.sleep(20)
