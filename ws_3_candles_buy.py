# Description in the these 2 videos from PartTimeLarry:
    # https://www.youtube.com/watch?v=Uv0jcZNYh5Q
    # https://www.youtube.com/watch?v=3maolsW1HqA

import websocket, json, requests, sys
from Alpaca_config import *
from dateutil.parser import parse
from datetime import datetime

minutes_processed = {}
minute_candlesticks = []
current_tick = None
previous_tick = None
in_position = False

bars_stream = ['SPY']
# quotes_stream = ["SPY"]

def on_open(ws):
    print("opened")
    auth = {
            "action": "auth",
            "key": API_KEY_PAPER,
            "secret": API_SECRET_PAPER
        }
    
    ws.send(json.dumps(auth)) # json.dumps convert json to string
    
    message = {
                "action": "subscribe",
                # "quotes": quotes_stream,
                "bars": bars_stream
            }              
    ws.send(json.dumps(message))


def on_message(ws, message):
    current_tick = json.loads(message)[0]
    print(current_tick)
    print("{} @ {}".format(current_tick['t'], current_tick['c']))
    tick_datetime_object = parse(current_tick['t'])
    tick_dt = tick_datetime_object.strftime('%Y-%m-%d %H:%M')

    print(tick_datetime_object.minute) # print current minute
    print(tick_dt)

    minute_candlesticks.append({
        "minute": tick_dt,
        "open": current_tick['o'],
        "high": current_tick['h'],
        "low": current_tick['l'],
        "close": current_tick['c'],
        "volume": current_tick['v'],
        "trades": current_tick['n'],
    })

    print("There were {} bars collected".format(len(minute_candlesticks)))  

    if len(minute_candlesticks) > 3:
        print("== there are more than 3 candlesticks, checking for pattern ==")
        last_candle = minute_candlesticks[-1]
        previous_candle = minute_candlesticks[-2]
        previous_previous_candle = minute_candlesticks[-3]

        print("== let's compare the last 3 candle closes ==")
        if last_candle['close'] > previous_candle['close'] and previous_candle['close'] > previous_previous_candle['close']:
            print("=== Three green candlesticks in a row, let's make a trade! ===")
            distance = last_candle['close'] - previous_previous_candle['open']
            print("Distance is {}".format(distance))
            profit_price = last_candle['close'] + (distance * 2)
            print("I will take profit at {}".format(profit_price))
            loss_price = previous_previous_candle['open']
            print("I will sell for a loss at {}".format(loss_price))

            '''
            if not in_position:
                print("== Placing order and setting in position to true ==")
                in_position = True
                place_order(profit_price, loss_price)
                sys.exit()
            '''
        else:
            print("No go")


'''
# Below is the version from PartTimeLarry of collecting every tick to the minute bar
def on_message(ws, message):
    global current_tick, previous_tick, in_position

    previous_tick = current_tick
    current_tick = json.loads(message)[0]

    print(current_tick)
    print("=== Received Tick ===")
    print("{} @ {}".format(current_tick['t'], current_tick['bp']))
    tick_datetime_object = datetime.utcfromtimestamp(current_tick['t']/1000)
    tick_dt = tick_datetime_object.strftime('%Y-%m-%d %H:%M')

    print(tick_datetime_object.minute) # print current minute
    print(tick_dt)

    if not tick_dt in minutes_processed:
        print("starting new candlestick")
        minutes_processed[tick_dt] = True
        print(minutes_processed)
    
        if len(minute_candlesticks) > 0: # getting close of the previous candlestick
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
'''

def on_close(ws):
    print("closed connection")

endpoint_stream = "wss://stream.data.alpaca.markets/v2/sip" # I have a paid subscription

ws = websocket.WebSocketApp(endpoint_stream, on_open=on_open, on_message=on_message, on_close=on_close)
ws.run_forever()