
# Store stream data in SQL DB + retrieve
# No SDK, just websocket (from Udemy)

import websocket
import json
import sqlite3 # standard python package
import datetime as dt
import dateutil.parser


endpoint_stream = "wss://stream.data.alpaca.markets/v2/sip" # I have a paid subscription
headers = json.loads(open("Alpaca_config.txt",'r').read())


# Storing ticks in SQL DB -------------------------------------------------------------------------

from Universes import Famous
streams = Famous # list of tickers

# Creating 2 blank sql dbs for storing quotes and trades separately
alpaca_trades_ticks_test_DB = sqlite3.connect('D:/Data/tick_data/alpaca_trades_ticks_test.db') # it is sql request, so it goes with "/" for the path
alpaca_quotes_ticks_test_DB = sqlite3.connect('D:/Data/tick_data/alpaca_quotes_ticks_test.db')

def create_tables(db, tickers, tick_type):
    c = db.cursor()
    if tick_type == "trades":
        for ticker in tickers: # creating separate table for every ticker
            c.execute("CREATE TABLE IF NOT EXISTS t{} (timestamp datetime primary key, 
                                                       trade_id integer, 
                                                       exchange text, 
                                                       price real(15,5), 
                                                       volume integer)".format(ticker))
    if tick_type == "quotes":
        for ticker in tickers:
            c.execute("CREATE TABLE IF NOT EXISTS q{} (timestamp datetime primary key, 
                                                        ask_exch text, 
                                                        ask_price real(15,5), 
                                                        ask_volume integer, 
                                                        bid_exch text, 
                                                        bid_price real(15,5), 
                                                        bid_volume integer)".format(ticker))
    try:
        db.commit()
    except:
        db.rollback()
        
create_tables(alpaca_trades_ticks_test_DB,streams,"trades") # separate table for every ticker
create_tables(alpaca_quotes_ticks_test_DB,streams,"quotes") # separate table for every ticker


def insert_ticks(datapoint):
    for tick in datapoint: # Multiple data points may arrive in each message received from the server
        tabl = tick['S'] # name of db table corresponds to the ticker
        if tick['T'] == "q": # if a datapoint is a quote => we write it to quote DB
            c = alpaca_quotes_ticks_test_DB.cursor()
            vals = [dateutil.parser.isoparse(tick['t']),tick["ax"],tick["ap"],tick["as"],tick["bx"],tick["bp"],tick["bs"]]
            query = "INSERT INTO q{}(timestamp,ask_xch,ask_price,ask_volume,bid_xch,bid_price,bid_volume) VALUES (?,?,?,?,?,?,?)".format(tabl)
            c.execute(query,vals)
            try:
                alpaca_quotes_ticks_test_DB.commit()
                print('Success wright to {}').format(tabl)
            except:
                alpaca_quotes_ticks_test_DB.rollback()
        if tick['T'] == "t": # if a datapoint is a trade => we write it to trades DB
            c = alpaca_trades_ticks_test_DB.cursor()
            vals = [dateutil.parser.isoparse(tick['t']),tick["i"],tick["x"],tick["p"],tick["s"]]
            query = "INSERT INTO t{}(timestamp,trade_id,x_code,price,volume) VALUES (?,?,?,?,?)".format(tabl)
            c.execute(query,vals)
            try:
                alpaca_trades_ticks_test_DB.commit()
                print('Success wright to {}').format(tabl)
            except:
                alpaca_trades_ticks_test_DB.rollback()

def on_open(ws):
    auth = {
            "action": "auth",
            "key": headers["APCA-API-KEY-ID"],
            "secret": headers["APCA-API-SECRET-KEY"]
           }
    
    ws.send(json.dumps(auth)) # json.dumps convert json to string
    
    message = {
                "action": "subscribe",
                "trades": streams,
                "quotes": streams,
                # "bars": streams
              }
                
    ws.send(json.dumps(message))


def on_message(ws, message):
    print(message)
    datapoint = json.loads(message)
    insert_ticks(datapoint)  

ws = websocket.WebSocketApp(endpoint_stream, on_open=on_open, on_message=on_message)
ws.run_forever()

# -------------------------------------------------------------------------------------------

# run storing code in the background (just run anaconda prompt and do "python name_of_file.py")...
# ... meanwhile it could still access the databases by e.g. code below

import sqlite3

alpaca_trades_ticks_test_DB = sqlite3.connect('D:/Data/tick_data/alpaca_trades_ticks_test.db') # it is sql request, so it goes with "/" for the path
c = db.cursor()

#check all table names
c.execute("SELECT name from sqlite_master where type = 'table' ")
c.fetchall()

#check the table structure for a given table
c.execute("PRAGMA table_info(AAPL)")
c.fetchall()

#fetch rows from a table
c.execute("SELECT * FROM AAPL WHERE price > 134.5")
c.fetchall()

# -------------------------------------------------------------------------------------------
# get tick data from SQL db to dataframe and resample it to 1min bars
import sqlite3
import pandas as pd

db = sqlite3.connect('D:/Alpaca/4_streaming/ticks.db')

def get_bars(db, ticker, day, period):
    data = pd.read_sql("SELECT * FROM {} WHERE timestamp >= date() - '{} days'".format(ticker, day), con = db)
    data.set_index(['timestamp'], inplace=True)
    data.index = pd.to_datetime(data.index) # converting strings to datetime
    price_ohlc = data.loc[:,['price']].resample(period).ohlc().dropna()
    price_ohlc.columns = ["open","high","low","close"]
    vol_ohlc = data.loc[:,['volume']].resample(period).apply({'volume':'sum'}).dropna()
    df = price_ohlc.merge(vol_ohlc, left_index=True, right_index=True)
    return df

get_bars(db,"TSLA",5, '1min')



 