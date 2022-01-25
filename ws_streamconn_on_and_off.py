"""
In this example code we will show how to shut the streamconn websocket
connection down and then up again. it's the ability to stop/start the
connection.
    - Could be very handy:
        -- during night time when the stream is not active, 
        -- or when we want to upgrade our environment.

conn.run() method is blocking, meaning we cannot execute anything after that. 
So in order to communicate with our conn object we need to use another thread. 
We then use the stop_ws() method that signals the connection to stop. 
"""
import logging
import threading
import asyncio
import time
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL

ALPACA_API_KEY = "<YOUR-API-KEY>"
ALPACA_SECRET_KEY = "<YOUR-SECRET-KEY>"

async def print_trade(t):
    print('trade', t)


async def print_quote(q):
    print('quote', q)


async def print_bar(bar):
    print('bar', bar)


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
    conn.run()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
                        level=logging.INFO)

    loop = asyncio.get_event_loop()

    while 1:
        try:
            threading.Thread(target=consumer_thread).start()
                                # conn.run() method is blocking, meaning we cannot execute anything after that 
                                # So in order to communicate with our conn object we need to use another thread
            time.sleep(20)
            loop.run_until_complete(conn.stop_ws())
            time.sleep(20)
        except:
            pass
