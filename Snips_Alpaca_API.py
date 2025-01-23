import requests
import os
import json
import pandas as pd
import time
import datetime as dt


# API
    # Source: https://www.udemy.com/course/algorithmic-trading-on-alpacas-platform-deep-dive/

    headers = json.loads(open("Alpaca_config.txt",'r').read())
    from Alpaca_config import *

    ord_url = endpoint_not_data_paper + "/v2/orders"
    pos_url = endpoint_not_data_paper + "/v2/positions"
    acc_url = endpoint_not_data_paper + "/v2/account"
   

# Account information

        # with API (no SDK)
            def get_account():
                r = requests.get(acc_url,headers=headers)
                return r.json()

            get_account() #get exhasutive information about your account
            float(get_account()["equity"]) #get only the relavant account information


# Orders

            # With plain API (no SDK)
                    def market_order(symbol, quantity, side="buy", tif="day"): #params w/o default go first
                        params = {"symbol": symbol,"qty": quantity,"side" : side,
                                "type": "market", # fixed
                                "time_in_force": tif}
                        r = requests.post(ord_url, headers=headers, json=params) # we do "post" as we send info; not "params=...", but "json=..."
                        return r.json()

                    def limit_order(symbol, quantity, limit_pr, side="buy", tif="day"):
                        params = {"symbol": symbol,"qty": quantity,"side" : side,
                                "type": "limit", # fixed
                                "limit_price" : limit_pr,
                                "time_in_force": tif}
                        r = requests.post(ord_url, headers=headers, json=params) # we do "post" as we send info; not "params=...", but "json=..."
                        return r.json()
                        
                    def stop_order(symbol, quantity, stop_pr, side="buy", tif="day"):
                        params = {"symbol": symbol,"qty": quantity,"side" : side,
                                "type": "stop", # fixed
                                "stop_price": stop_pr,
                                "time_in_force": tif}
                        r = requests.post(ord_url, headers=headers, json=params) # we do "post" as we send info; not "params=...", but "json=..."
                        return r.json()

                    def stop_limit_order(symbol, quantity, stop_pr, limit_pr, side="buy", tif="day"):
                        params = {"symbol": symbol,"qty": quantity,"side" : side,
                                "type": "stop_limit", # fixed
                                "stop_price": stop_pr,
                                "limit_price" : limit_pr,
                                "time_in_force": tif}
                        r = requests.post(ord_url, headers=headers, json=params) # we do "post" as we send info; not "params=...", but "json=..."
                        return r.json()

                    def trail_stop_order(symbol, quantity, trail_pr, side="buy", tif="day"):
                        params = {"symbol": symbol, "qty": quantity, "side" : side,
                                "type": "trailing_stop",
                                "trail_price" : trail_pr,
                                "time_in_force": tif}
                        r = requests.post(ord_url, headers=headers, json=params) # we do "post" as we send info; not "params=...", but "json=..."
                        return r.json()

                    def bracket_order(symbol, quantity, tplp, slsp, sllp, side="buy", tif="day"):
                        params = {
                                "symbol": symbol,"qty": quantity,"side" : side,
                                "type": "market", # fixed
                                "time_in_force": tif,
                                "order_class": "bracket", 
                                "take_profit" : { # becomes nested
                                                "limit_price":tplp
                                                },
                                "stop_loss" : { # becomes nested
                                                "stop_price": slsp,
                                                "limit_price": sllp
                                                }
                                }
                        r = requests.post(ord_url, headers=headers, json=params) # we do "post" as we send info; not "params=...", but "json=..."
                        return r.json()


                    market_order("AMZN", 1) 
                    limit_order("AMZN", 1, limit_pr = 3202)
                    stop_order("AMZN", 1, 3185, "sell")
                    stop_limit_order("AMZN", 1, 3175, 3175, "sell")

                    market_order("AAPL", 1)
                    trail_stop_order("AAPL", 1, 2, "sell")

                    bracket_order("AAPL", 1, 139, 126, 126)


        # Order list and executed trades

                # with API (no SDK)
                    def order_list(status = "open", limit = 50):
                        params = {"status":status} 
                        r = requests.get(ord_url, headers=headers, params=params) # we do "get" as we want to receive info
                        data = r.json()
                        return pd.DataFrame(data)

                    order_df = order_list("closed") 
                

        # Cancel / replace orders

                # via pure API (no SDK):
                    def order_cancel(order_id=""): # if blank - we cancel all not-filled orders
                        if len(order_id)>1:
                            ord_cncl_url = ord_url+"/{}".format(order_id)
                        else:
                            ord_cncl_url = ord_url
                        r = requests.delete(ord_cncl_url, headers=headers) # we do "delete"
                        return r.json()

                    def order_replace(order_id, params):
                        ord_replc_url = ord_url+"/{}".format(order_id)
                        r = requests.patch(ord_replc_url, headers=headers, json=params) # we do "patch" as we want to change
                        return r.json()

                    order_df = order_list()
                    order_cancel(order_df[order_df["symbol"]=="CSCO"]["id"].to_list()[0]) # how to select a specific order ID: e.g. newest for CSCO
                    order_cancel()
                    order_replace(order_df[order_df["symbol"]=="FB"]["id"].to_list()[0],
                                {"qty" : 10, "trail": 3}) # replace existing trail order: chg qty and trail


# Position & portfolio

        # Current position

            # via pure API (no SDK)
                def positions(symbol=""):
                    if len(symbol)>1:
                        r = requests.get(pos_url+"/{}".format(symbol), headers=headers)
                        return r.json() # no need to return a dataframe
                    else:
                        r = requests.get(pos_url, headers=headers)
                        return pd.DataFrame(r.json())

                positions()



        # Close all positions

            # via pure API (no SDK)
                def del_positions(symbol="", qty=0):
                    if len(symbol)>1:
                        pos_url = endpoint_not_data_paper + "/v2/positions/{}".format(symbol)
                        params = {"qty" : qty}
                    else:
                        params = {}
                    r = requests.delete(pos_url, headers=headers, json=params)
                    return r.json()

                del_positions("CSCO") #delete CSCO position
                del_positions() #delete all positions



# Quotes and bars

    # v1 data api

        def hist_data_v1(symbols, timeframe="15Min", limit=200, start="", end="", after="", until=""):
            df_data = {}
            bar_url = endpoint_data_v1 + "/bars/{}".format(timeframe)
            params = {"symbols" : symbols, # symbols should be in a string format separated by comma e.g. symbols = "MSFT,AMZN,GOOG"
                    "limit" : limit,
                    "start" : start,
                    "end" : end,
                    "after" : after,
                    "until" : until}
            r = requests.get(bar_url, headers = headers, params = params) # we do "get" as we want to receive info
            json_dump = r.json()
            for symbol in json_dump:
                temp = pd.DataFrame(json_dump[symbol])
                temp.rename({"t":"time","o":"open","h":"high","l":"low","c":"close","v":"volume"},axis=1, inplace=True)
                temp["time"] = pd.to_datetime(temp["time"], unit="s") # Alpaca shows time in Unix format (seconds) in UTC
                temp.set_index("time",inplace=True)
                temp.index = temp.index.tz_localize("UTC").tz_convert("America/Indiana/Petersburg") # localize mean you say that initial time is in UTC
                df_data[symbol] = temp
                return df_data
                
        data_dump = hist_data_v1("FB,CSCO,AMZN", timeframe="5Min")  # do not put space btw tickers

        # Resample example: 5Min into 1Hour
        data_1h = {}
        for ticker in data_dump:
            logic = {'open'  : 'first',
                    'high'  : 'max',
                    'low'   : 'min',
                    'close' : 'last',
                    'volume': 'sum'}
            data_1h[ticker] = data_dump[ticker].resample('1H').apply(logic)
            data_1h[ticker].dropna(inplace=True)



        tickers = ["FB", "AMZN", "GOOG"]
        starttime = time.time()
        timeout = starttime + 60*5
        while time.time() <= timeout:
            print("*****************************************************************")
            for ticker in tickers:
                print("printing data for {} at {}".format(ticker,time.time()))
                print(hist_data_v1(ticker, timeframe="1Min"))
            time.sleep(60 - ((time.time() - starttime) % 60))

        # Last trade and quote
        # Below is the implementation for V1 API, check V2

            def last_trade(symbol):
                last_trade_url = endpoint_data_v1 + "/last/stocks/{}".format(symbol) # V1 endpoint
                r = requests.get(last_trade_url, headers = headers) # we do "get" as we want to receive info 
                return (r.json()["last"]["price"], r.json()["last"]["size"])

            price,volume = last_trade("CSCO")

            def last_quote(symbol):
                last_quote_url = endpoint_data_v1 + "/last_quote/stocks/{}".format(symbol) # V1 endpoint
                r = requests.get(last_quote_url, headers = headers) # we do "get" as we want to receive info
                return r.json()

            last_quote("CSCO")


    # v2 data api

        # via pure API (no SDK)
            symbols = ["FB","CSCO","AMZN"]

            def hist_data_v2(symbols, start="2021-01-01", timeframe="1Hour", limit=600, end=""):
                """
                returns historical bar data for a string of symbols separated by comma
                symbols should be in a string format separated by comma e.g. symbols = "MSFT,AMZN,GOOG"
                """
                df_data_tickers = {}
                
                for symbol in symbols:   
                    bar_url = endpoint_data_v2 + "/stocks/{}/bars".format(symbol)
                    params = {"start":start, "limit" :limit, "timeframe":timeframe}
                    
                    data = {"bars": [], "next_page_token":'', "symbol":symbol}
                    while True: # we loop through until all next_page_tokens are through
                            r = requests.get(bar_url, headers = headers, params = params) # we do "get" as we want to receive info
                            r = r.json()
                            if r["next_page_token"] == None:
                                data["bars"]+=r["bars"]
                                break # no more next_page_tokens
                            else:
                                params["page_token"] = r["next_page_token"]
                                data["bars"]+=r["bars"]
                                data["next_page_token"] = r["next_page_token"]
                    
                    df_data = pd.DataFrame(data["bars"])
                    df_data.rename({"t":"time","o":"open","h":"high","l":"low","c":"close","v":"volume"},axis=1, inplace=True)
                    df_data["time"] = pd.to_datetime(df_data["time"])
                    df_data.set_index("time",inplace=True)
                    df_data.index = df_data.index.tz_convert("America/Indiana/Petersburg")
                    
                    df_data_tickers[symbol] = df_data
                return df_data_tickers
                    
            data_dump = hist_data_v2(symbols, start="2021-05-15", timeframe="1Min")  
