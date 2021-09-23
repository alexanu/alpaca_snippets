import requests
import os
import json
import pandas as pd


# API
    # Source: https://www.udemy.com/course/algorithmic-trading-on-alpacas-platform-deep-dive/

    headers = json.loads(open("Alpaca_config.txt",'r').read())

    endpoint_not_data_paper = "https://paper-api.alpaca.markets" # this is paper, think how to change it to live
    ord_url = endpoint_not_data_paper + "/v2/orders"
    pos_url = endpoint_not_data_paper + "/v2/positions"
    acc_url = endpoint_not_data_paper + "/v2/account"


    endpoint_data_v1 = "https://data.alpaca.markets/v1"
    endpoint_data_v2 = "https://data.alpaca.markets/v2"


# SDK
    import alpaca_trade_api as tradeapi
    from alpaca_trade_api.rest import TimeFrame


    from Alpaca_config import *
    alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
    alpaca.get_data_stream_url()


# Account information

        def get_account():
            r = requests.get(acc_url,headers=headers)
            return r.json()

        get_account() #get exhasutive information about your account
        float(get_account()["equity"]) #get only the relavant account information

        account = alpaca.get_account()
        print(account.status,account.buying_power)
        alpaca.get_account_configurations()

        portfolio_cash = float(alpaca.get_account().cash)
        buyingpower = float(account.buying_power)
        balance_change = float(account.equity) - float(account.last_equity) # Check our current balance vs. our balance at the last market close


        # Figure out how much money we have to work with, accounting for margin
            account_info = alpaca.get_account()
            equity = float(account_info.equity)
            margin_multiplier = float(account_info.multiplier)
            total_buying_power = margin_multiplier * equity
            print(f'Initial total buying power = {total_buying_power}')


# Orders
        # Submit orders

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


                alpaca.submit_order(symbol='AAPL',qty=1,side='buy',type='market',time_in_force='gtc',client_order_id='FU')
                    # limit_price=None, stop_price=None, order_class=None, take_profit=None, stop_loss=None, trail_price=None, trail_percent=None, notional=None)

                strategy_name = "Break_out_10min"
                api.submit_order(symbol='AAPL',qty=100,
                                client_order_id= strategy_name + "_" + str(int(time.mktime(api.get_clock().timestamp.timetuple()))), # unique client order
                                side=direction,type='market',time_in_force='day')


                def submitOrder(self, qty, stock, side, resp):
                    if(qty > 0):
                        try:
                            alpaca.submit_order(stock, qty, side, "market", "day")
                            print("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
                            resp.append(True)
                        except:
                            print("Order of | " + str(qty) + " " + stock + " " + side + " | did not go through.")
                            resp.append(False)
                    else:
                        print("Quantity is 0, order of | " + str(qty) + " " + stock + " " + side + " | not completed.")
                        resp.append(True)


                def place_order(self, symbol:str, dollar_amount:float):
                    if np.sign(dollar_amount) > 0:
                        side = 'buy'
                    elif np.sign(dollar_amount) < 0:
                        side = 'sell'
                    current_asset_price = api.get_last_trade(symbol).price
                    qty = int(abs(dollar_amount) / current_asset_price)
                    if qty > 0:
                        order = api.submit_order(symbol=symbol,
                                                qty=qty,
                                                side=side,
                                                type='market',
                                                time_in_force='day')


        # Order list

                def order_list(status = "open", limit = 50):
                    params = {"status":status} 
                    r = requests.get(ord_url, headers=headers, params=params) # we do "get" as we want to receive info
                    data = r.json()
                    return pd.DataFrame(data)

                order_df = order_list("closed") # full list of statuses: https://alpaca.markets/docs/trading-on-alpaca/orders/#order-lifecycle
                
                orders = alpaca.list_orders(status="open")

                symbol = ['SPY']
                order = [o for o in alpaca.list_orders() if o.symbol == symbol]

                open_orders = alpaca.list_orders(status='open',until=date.today())
                open_sell_symbols = {order.symbol for order in open_orders if order.side == "sell" and order.filled_at == None}
                
                filled_orders = alpaca.list_orders(status="filled")
                [print(o.id) for o in filled_orders]

                alpaca.get_order(order_id)
                my_order = alpaca.get_order_by_client_order_id('my_first_order')

                alpaca.get_activities()

                alpaca.get_trades("AAPL", "2021-02-08", "2021-02-08", limit=10).df


        # Cancel / replace orders

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

                alpaca.cancel_order(order_id)
                alpaca.cancel_all_orders()


        # update position
            def send_order(self, target_qty):
                if current_order is not None: # We don't want to have two orders open at once
                    alpaca.cancel_order(current_order.id)

                delta = target_qty - position
                if delta == 0:
                    return
                print(f'Ordering towards {target_qty}...')
                try:
                    if delta > 0:
                        buy_qty = delta
                        if position < 0:
                            buy_qty = min(abs(position), buy_qty)
                        print(f'Buying {buy_qty} shares.')
                        current_order = alpaca.submit_order(symbol, buy_qty, 'buy','limit', 'day', last_price)
                    elif delta < 0:
                        sell_qty = abs(delta)
                        if position > 0:
                            sell_qty = min(abs(position), sell_qty)
                        print(f'Selling {sell_qty} shares.')
                        current_order = alpaca.submit_order(symbol, sell_qty, 'sell','limit', 'day', last_price)
                except Exception as e:
                    print(e)


        # Buy couple minutes before market close
            clock = alpaca.get_clock()
            if clock.is_open:
                time_until_close = clock.next_close - clock.timestamp # Wait to buy
                if time_until_close.seconds <= 120:
                    print('Buying positions...')
                    portfolio_cash = float(alpaca.get_account().cash)
                    quantity = portfolio_cash / price
                    alpaca.submit_order(symbol=symbol,qty=quantity,side='buy',type='market',time_in_force='day')
                    print('Positions bought.')


# Assets
    # 'shortable' = True, 'easy_to_borrow' = True, 'marginable' = True
    alpaca.list_assets(status="active", asset_class=None)

    assets = alpaca.list_assets(status="active")
    assets = [asset for asset in assets if asset.tradable ]

    active_assets = alpaca.list_assets(status='active')

    nasdaq_assets = [a for a in active_assets if a.exchange == 'NASDAQ']
    amex_assets = [b for b in active_assets if b.exchange == 'AMEX']
    nyse_assets = [c for c in active_assets if c.exchange == 'NYSE']
    arca_assets = [d for d in active_assets if d.exchange == 'ARCA']

    alpaca.get_asset('MSFT').shortable*alpaca.get_asset('GME').easy_to_borrow
    alpaca.get_asset('MSFT').easy_to_borrow

    alpaca.get_snapshots('MSFT')


    assets = api.list_assets(status=None, asset_class=None)
    assets_viable = []
    for a in assets:        
        if getattr(a,'class') == 'us_equity' and getattr(a,'fractionable')==True and getattr(a,'status')=='active' and getattr(a,'tradable')==True:
            assets_viable.append(getattr(a,'symbol'))


# Position

        # Current position

            def positions(symbol=""):
                if len(symbol)>1:
                    r = requests.get(pos_url+"/{}".format(symbol), headers=headers)
                    return r.json() # no need to return a dataframe
                else:
                    r = requests.get(pos_url, headers=headers)
                    return pd.DataFrame(r.json())

            positions()

            positions = alpaca.list_positions().Position[1]
            len(positions[1].change_today)

            try:
                position = int(alpaca.get_position("SPY").qty)
            except: # No position exists
                position = 0

            positions = {position.symbol for position in alpaca.list_positions()}
            symbol = ['SPY']
            position = [p for p in alpaca.list_positions() if p.symbol == symbol]


            def get_num_shares(stock):
                all_pos = api.list_positions()
                i = 0
                for i in range(len(all_pos)):
                    if stock == all_pos[i].symbol:
                        return all_pos[i].qty
                return 0 # If we don't find it in all_pos


            pos = api.get_position(symbol)
            returns = (float(pos.current_price)/float(pos.avg_entry_price)) - 1
            value = float(pos.current_price) * int(pos.qty)


        # Close all positions

            def del_positions(symbol="", qty=0):
                if len(symbol)>1:
                    pos_url = endpoint + "/v2/positions/{}".format(symbol)
                    params = {"qty" : qty}
                else:
                    params = {}
                r = requests.delete(pos_url, headers=headers, json=params)
                return r.json()

            del_positions("CSCO") #delete CSCO position
            del_positions() #delete all positions

            alpaca.close_all_positions()


            import threading
            positions = alpaca.list_positions()
            for position in positions:
                if(position.side == 'long'):
                    orderSide = 'sell'
                else:
                    orderSide = 'buy'
                qty = abs(int(float(position.qty)))
                respSO = []
                tSubmitOrder = threading.Thread(target=submitOrder(qty, position.symbol, orderSide, respSO))
                tSubmitOrder.start()
                tSubmitOrder.join()


            # Take profit
                portfolio = api.list_positions()
                for position in portfolio:
                    profit = position.unrealized_pl
                    percentChange = (profit/position.cost_basis) * 100
                    if (percentChange > 5):
                        print("Selling {} shares of {}".format(position.qty,position.symbol))
                        api.submit_order(symbol=position.symbol,qty=position.qty,side='sell',type='market',time_in_force='opg')


            # Sell everything a minute after the market opens
                clock = alpaca.get_clock()
                time_after_open = clock.next_open - clock.timestamp
                if time_after_open.seconds >= 60:
                    print('Liquidating positions.')
                    alpaca.close_all_positions()


# Quotes and bars

    # v1 data api

        def hist_data_v1(symbols, timeframe="15Min", limit=200, start="", end="", after="", until=""):
            """
            returns historical bar data for a string of symbols separated by comma
            symbols should be in a string format separated by comma e.g. symbols = "MSFT,AMZN,GOOG"
            """
            df_data = {}
            bar_url = endpoint_data_v1 + "/bars/{}".format(timeframe)
            params = {"symbols" : symbols,
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



        alpaca.get_barset("AAPL", "5Min", "limit", start="2021-08-30", end="2021-08-30")
        # timeframe: minute, 1Min, 5Min, 15Min, day or 1D. 
        # start, end, 
        # after, until need to be string format, which you can obtain with pd.Timestamp().isoformat() 
        
        # if you are using limit, it is calculated from the end date. 
        # if end date is not specified, "now" is used


        import pandas as pd
        NY = 'America/New_York'
        start=pd.Timestamp('2020-08-01', tz=NY).isoformat()
        end=pd.Timestamp('2020-08-30', tz=NY).isoformat()
        print(api.get_barset(['AAPL', 'GOOG'], 'day', start=start, end=end).df)


        start=pd.Timestamp('2020-08-28 9:30', tz=NY).isoformat()
        end=pd.Timestamp('2020-08-28 16:00', tz=NY).isoformat()
        print(api.get_barset(['AAPL', 'GOOG'], 'minute', start=start, end=end).df)


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


        # Resample example: 1Min into 5Min
        data_5m = {}
        for ticker in data_dump:
            logic = {'open'  : 'first',
                    'high'  : 'max',
                    'low'   : 'min',
                    'close' : 'last',
                    'volume': 'sum'}
            data_5m[ticker] = data_dump[ticker].resample('5Min').apply(logic)
            data_5m[ticker].dropna(inplace=True)






        alpaca.get_bars("AAPL", TimeFrame.Day, "2021-08-01", "2021-08-30", limit=10, adjustment='raw').df
    
        bars = alpaca.get_bars('AAPL', TimeFrame.Minute,
                                pd.Timestamp('now').date(),pd.Timestamp('now').date(), limit=300,adjustment='raw')


        from alpaca_trade_api.rest import TimeFrame
        now = pd.Timestamp.now(tz='America/New_York').floor('1min')
        market_open = now.replace(hour=9, minute=30)
        today = now.strftime('%Y-%m-%d')
        tomorrow = (now + pd.Timedelta('1day')).strftime('%Y-%m-%d')
        data = alpaca.get_bars(symbol, TimeFrame.Minute, today, tomorrow, adjustment='raw').df
        bars = data[market_open:]


        today = date.today()
        historical_final = pd.DataFrame()
        for i in range(5):        
            startyear = 5-i
            endyear = startyear - 1        
            startdate = datetime.datetime(today.year-startyear,today.month,today.day).strftime("%Y-%m-%d")
            enddate = datetime.datetime(today.year-endyear,today.month,today.day).strftime("%Y-%m-%d")
            historical = api.get_bars(symbol, TimeFrame.Hour, startdate, enddate, limit=10000, adjustment='raw').df
            historical = historical.reset_index()
            historical_final = pd.concat([historical_final,historical])    
        historical_final = historical_final.drop_duplicates()
        historical_final = historical_final.reset_index(drop=True)


        # Ranks all stocks by percent change over the past 10 minutes (higher is better).
            stockUniverse = ['DOMO', 'TLRY', 'SQ', 'MRO', 'AAPL', 'GM', 'SNAP', 'SHOP']
            allStocks = []
            for stock in stockUniverse:
                allStocks.append([stock, 0])

            def getPercentChanges():
                length = 10
                for i, stock in enumerate(allStocks):
                    bars = alpaca.get_bars(stock[0], TimeFrame.Minute,pd.Timestamp('now').date(),pd.Timestamp('now').date(), limit=length,adjustment='raw')
                    allStocks[i][1] = (bars[stock[0]][len(bars[stock[0]]) - 1].c - bars[stock[0]][0].o) / bars[stock[0]][0].o

            def rank():
                tGetPC = threading.Thread(target=getPercentChanges)
                tGetPC.start()
                tGetPC.join()


        # Last trade and quote
        # Replicate v2 from v1 implementation

            alpaca.get_last_quote("AAPL")
            alpaca.get_last_trade("AAPL")
            # Check here: https://alpaca.markets/learn/latest-trade-quote/

            alpaca.get_quotes("AAPL", "2021-02-08", "2021-02-08", limit=10).df


# Working with time and dates, market hours

    alpaca.get_calendar("2021-02-08", "2021-02-18") # start=None, end=None

    from alpaca_trade_api.rest import TimeFrame
    now = pd.Timestamp.now(tz='America/New_York').floor('1min')
    market_open = now.replace(hour=9, minute=30)
    today = now.strftime('%Y-%m-%d')
    tomorrow = (now + pd.Timedelta('1day')).strftime('%Y-%m-%d')
    data = alpaca.get_bars(symbol, TimeFrame.Minute, today, tomorrow, adjustment='raw').df
    bars = data[market_open:]

    # Check market status
        def check_market():
            print('\n')
            clock = alpaca.get_clock()
            print('The market is {}'.format('open.' if clock.is_open else 'closed.'))    
            print('\n')

        def _check_market_open(self):
            clock = alpaca.get_clock()
            if clock.is_open:
                pass
            else:
                time_to_open = clock.next_open - clock.timestamp
                print(f"Market is closed now going to sleep for ~{time_to_open.total_seconds()//3600} hours till {clock.next_open.ctime()}")
                time.sleep(time_to_open.total_seconds())

        def run(self):
            self._check_market_open()
            ...


    # Wait for market to open.
        import threading
        import time
        import datetime

        def awaitMarketOpen():
            isOpen = alpaca.get_clock().is_open
            while(not isOpen):
                clock = alpaca.get_clock()
                openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
                currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
                timeToOpen = int((openingTime - currTime) / 60)
                print(str(timeToOpen) + " minutes til market open.")
                time.sleep(60)
                isOpen = self.alpaca.get_clock().is_open

        print("Waiting for market to open...")
        tAMO = threading.Thread(target=awaitMarketOpen())
        tAMO.start()
        tAMO.join()
        print("Market opened.")


    # 15 minutes till market close
        import time
        import datetime

        clock = alpaca.get_clock()
        closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
        currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
        timeToClose = closingTime - currTime
        if(timeToClose < (60 * 15)):
            print("Market closing soon.  Closing positions.")
        print("Sleeping until market close (15 minutes).")
        time.sleep(60 * 15)


    # Wait till market opens
        def wait_for_market_open():
            clock = alpaca.get_clock()
            if not clock.is_open:
                time_to_open = (clock.next_open - clock.timestamp).total_seconds()
                sleep(round(time_to_open))

        def time_to_market_close():
            clock = alpaca.get_clock()
            closing = clock.next_close - clock.timestamp
            return round(closing.total_seconds() / 60)

        if time_to_market_close() > 120:
            wait_for_market_open()
    		

    # Buy couple minutes before market close
        clock = alpaca.get_clock()
        if clock.is_open:
            time_until_close = clock.next_close - clock.timestamp # Wait to buy
            if time_until_close.seconds <= 120:
                print('Buying positions...')
                portfolio_cash = float(alpaca.get_account().cash)
                quantity = portfolio_cash / price
                alpaca.submit_order(symbol=symbol,qty=quantity,side='buy',type='market',time_in_force='day')
                print('Positions bought.')


    # Sell everything a minute after the market opens
        clock = alpaca.get_clock()
        time_after_open = clock.next_open - clock.timestamp
        if time_after_open.seconds >= 60:
            print('Liquidating positions.')
            alpaca.close_all_positions()






