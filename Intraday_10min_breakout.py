import json
import logging
import threading
import pandas as pd
import datetime as dt
from time import *
import time

import alpaca_trade_api as tradeapi
from Alpaca_config import *
from utils import *
api = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, api_version='v2')

mail_subject = "Trading 10min range breakouts with Alpaca paper"
strategy_name = "Break_out_10min"


trade_msg = []
past_trades = []

searching_for_trade = False
order_sent = False
order_submitted = False
active_trade = False
done_for_the_day = False

# check if market is open
api.cancel_all_orders()
clock = api.get_clock()

if clock.is_open:
    pass
else:
    time_to_open = clock.next_open - clock.timestamp
    mail_content = f"Market is closed now going to sleep for ~{time_to_open.total_seconds()//3600} hours till {clock.next_open.ctime()}"
    print(mail_content)
    sent_alpaca_email(mail_subject, mail_content)
    sleep(time_to_open.total_seconds())

if len(api.list_positions()) == 0:
    searching_for_trade = True
else:
    active_trade = True

# init WebSocket
conn = tradeapi.stream2.StreamConn(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER)

@conn.on(r'^trade_updates$')
async def on_trade_updates(conn, channel, trade):
    trade_msg.append(trade)
    # if 'fill' in trade.event:
    #     past_trades.append([trade.order['updated_at'],trade.order['symbol'],trade.order['side'],trade.order['filled_qty'],trade.order['filled_avg_price']])
    #     with open('past_trades.csv', 'w') as f:
    #         json.dump(past_trades, f, indent=4)
    #     print(past_trades[-1])

def ws_start():
    conn.run(['trade_updates'])

# start WebSocket in a thread
ws_thread = threading.Thread(target=ws_start, daemon=True)
ws_thread.start()
sleep(10)


# functions
def time_to_market_close():
    clock = api.get_clock()
    closing = clock.next_close - clock.timestamp
    return round(closing.total_seconds() / 60)


def send_order(direction):
    if time_to_market_close() > 20: # send order only it is enough time till market close
        if direction == 'buy':
            sl = high - range_size
            tp = high + range_size
        elif direction == 'sell':
            sl = low + range_size
            tp = low - range_size

        api.submit_order(symbol='AAPL',qty=100,
                        client_order_id= strategy_name + "_" + str(int(time.mktime(api.get_clock().timestamp.timetuple()))), # unique client order
                        side=direction,type='market',time_in_force='day',order_class='bracket',
                        stop_loss=dict(stop_price=str(sl)),
                        take_profit=dict(limit_price=str(tp)))
        return True, False # order_sent, done_for_the_day
    else: # ...otherwise: done_for_the_day
        return False, True # order_sent, done_for_the_day



# main loop
while True:

    try:
        # deriving range of last 10min
        candlesticks = api.get_barset('AAPL', 'minute', limit=10)
        high = candlesticks['AAPL'][0].h
        low = candlesticks['AAPL'][0].l
        range_size = high - low
        if range_size / candlesticks['AAPL'][0].c < 0.003:
            range_size = candlesticks['AAPL'][0].c * 0.003
        for candle in candlesticks['AAPL']:
            if candle.h > high:
                high = candle.h
            elif candle.l < low:
                low = candle.l
        range_size = high - low

        while searching_for_trade: # if there are no open positions ...
            clock = api.get_clock()
            sleep(60 - clock.timestamp.second) # sleep till beginning of next minute
            candlesticks = api.get_barset('AAPL', 'minute', limit=1)
            if candlesticks['AAPL'][0].c > high: # go long if there is a new high
                searching_for_trade = False
                order_sent, done_for_the_day = send_order('buy')

            elif candlesticks['AAPL'][0].c < low: # short if a new low
                searching_for_trade = False
                order_sent, done_for_the_day = send_order('sell')

        while order_sent:
            sleep(1)
            for item in trade_msg:
                if item.event == 'new':
                    order_submitted = True
                    order_sent = False

        while order_submitted:
            sleep(1)
            for item in trade_msg:
                if item.order['filled_qty'] == '100':
                    order_submitted = False
                    active_trade = True
                    mail_content = "A {item.order['side']} order filled for {item.order['symbol']} at {item.order['filled_avg_price']}"
                    sent_alpaca_email(mail_subject, mail_content)
                    trade_msg = []

        while active_trade:
            for i in range(time_to_market_close() - 5):
                sleep(60)
                if len(api.list_positions()) == 0:
                    active_trade = False
                    searching_for_trade = True
                    break
            if active_trade:
                done_for_the_day = True
                active_trade = False

        while done_for_the_day:
            mail_content = f"Closed {len(api.list_positions())} positions"
            api.close_all_positions()
            clock = api.get_clock()
            next_market_open = clock.next_open - clock.timestamp
            mail_content = mail_content + f" and sleeping till {clock.next_open.ctime()}"
            sent_alpaca_email(mail_subject, mail_content)
            sleep(next_market_open.total_seconds())
            searching_for_trade = True

    except Exception as e:
        logging.exception(e)
