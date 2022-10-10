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


# SDK
    import Universes
    from Alpaca_config import *

    # Old SDK (maintained till 2022): https://github.com/alpacahq/alpaca-trade-api-python
        import alpaca_trade_api as tradeapi
        tradeapi.__version__
        from alpaca_trade_api.rest import TimeFrame
        alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
        alpaca = tradeapi.REST(API_KEY_REAL, API_SECRET_REAL, API_BASE_URL_REAL, 'v2')
    

# Account information

        # with API (no SDK)
            def get_account():
                r = requests.get(acc_url,headers=headers)
                return r.json()

            get_account() #get exhasutive information about your account
            float(get_account()["equity"]) #get only the relavant account information

        alpaca.get_account_configurations()

        account = alpaca.get_account()
        print(account.status,account.buying_power, account.multiplier)

        portfolio_cash = float(account.cash) # account.cash value should generally not be used for much of anything. 
                                                          # it becomes meaningless if one ever shorts a position
        buyingpower = float(account.buying_power) # buying power is 2x equity if 2000 < equity < 25000, and 4x equity if equity > 25000
        print(f'Initial total buying power = {buyingpower}')
        
        # Check our current balance vs. our balance at the last market close
        balance_change = float(account.equity) - float(account.last_equity) 


        is_day_trade = True
        buying_power = float(account.daytrading_buying_power) if is_day_trade else float(account.regt_buying_power)


# Orders
        # https://alpaca.markets/docs/trading-on-alpaca/orders/

        # Submit orders
            # Nice overview of different order types: https://alpaca.markets/learn/13-order-types-you-should-know-about/
            # From Feb 2022 Alpaca allows trading from 4am till 8pm ET (10am - 2am Munich Time)
                import math
                limit_price = str(math.ceil(float(alpaca.get_latest_trade("SPY").p))) # round up is done with math.ceil()
                alpaca.submit_order(symbol="SPY", 
                                    qty=15, 
                                    side="buy", 
                                    type="limit", 
                                    limit_price=limit_price,
                                    time_in_force='day',
                                    extended_hours=True) # from 4am till 8pm ET (10am - 2am Munich Time)
                        # Any other order types, including market orders, will be rejected
                        # Your extended hour order will be processed and filled immediately.

            # Orders not eligible for extended hours 
            #       submitted between 4:00pm - 7:00pm ET will be rejected.
            #       submitted after 7:00pm ET will be queued and eligible for execution at the time of the next market open

            # There are special rules for submitting orders for extended hours: https://alpaca.markets/docs/trading-on-alpaca/orders/#extended-hours-trading

            # A limit orders with a limit price that significantly exceeds the current market price will be rejected

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


            coid = str(int(time.mktime(alpaca.get_clock().timestamp.timetuple())))
            alpaca.submit_order(symbol='AAPL',qty=1,side='buy',type='market',time_in_force='gtc',client_order_id=coid)

            # Time in force: https://alpaca.markets/docs/trading-on-alpaca/orders/#time-in-force
                '''
                gtc - good-till-cancelled
                day - eligible for execution only on the day it is live (9:30am - 4:00pm ET)
                opg - use it to submit “market on open” (MOO) and “limit on open” (LOO) orders
                cls - use it to submit “market on close” (MOC) and “limit on close” (LOC) orders
                ioc - Immediate Or Cancel (IOC) - all or part of the order to be executed immediately
                fok - Fill or Kill (FOK)
                '''

            strategy_name = "Break_out_10min"
            coid = strategy_name + "_" + str(int(time.mktime(alpaca.get_clock().timestamp.timetuple())))
            alpaca.submit_order(symbol='AAPL',qty=100,
                            client_order_id= coid, # unique client order
                                    # client-side unique order ID will be automatically generated by the system if not provided by the client
                            side='buy',
                            type='market',
                            time_in_force='day')


            def place_order(self, symbol:str, dollar_amount:float):
                if np.sign(dollar_amount) > 0:
                    side = 'buy'
                elif np.sign(dollar_amount) < 0:
                    side = 'sell'
                current_asset_price = math.ceil(float(alpaca.get_latest_trade(symbol).p)) # round up is done with math.ceil()
                qty = int(abs(dollar_amount) / current_asset_price)
                if qty > 0:
                    alpaca.submit_order(symbol=symbol,qty=qty,side=side,type='market',time_in_force='day')
                if(qty > 0):
                    try:
                        alpaca.submit_order(stock, qty, side, "market", "day")
                        print("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
                    except:
                        print("Order of | " + str(qty) + " " + stock + " " + side + " | did not go through.")
                else:
                    print("Quantity is 0, order of | " + str(qty) + " " + stock + " " + side + " | not completed.")


            # Place trailing stops for all tickers owned
                positions = alpaca.list_positions()
                for position in positions:
                    try:
                        # Set a trailing stop for it
                        alpaca.submit_order(symbol=position.symbol,qty=position.qty,side='sell',type='trailing_stop',trail_percent=10, time_in_force='gtc')
                        print(f"Placed a trailing stop order for {order.symbol}")
                    except Exception as e:
                        print(f"Unable to place sell order for {order.symbol}")
                        print(e)


            ticker = random.choice(Universes.Spiders)
            current_asset_price = math.ceil(float(alpaca.get_latest_trade(ticker).p)) # round up is done with math.ceil()
            qty = int(float(account.buying_power)/100 / current_asset_price)
            alpaca.submit_order(symbol=ticker, qty=qty, side="buy", type="limit", limit_price=str(current_asset_price), time_in_force='day', extended_hours=True) # from 4am till 8pm ET (10am - 2am Munich Time)
            print("Bought {} of {}. Buing power left is {}".format(qty, ticker, float(account.buying_power)))



            '''
            How to switch from long to short? 
            2 orders are required: 
                (Option A) Send 1st order. Listen on web socket for order ack. Then send 2nd order. 
                (Option B) Send 1st order. Sleep for some configurable amount of time. Then send 2nd order. Retry 2nd order if rejected for ‘invalid qty’. 
            '''


        # Order list and executed trades

                # full list of statuses: https://alpaca.markets/docs/trading-on-alpaca/orders/#order-lifecycle
                    # Updates on orders states at Alpaca will be sent over the streaming interface
                orders = alpaca.list_orders(status="all") # open, closed, all, filled
                alpaca.list_orders(status="filled",symbols=['AAPL','SPY'])
                    # https://docs.alpaca.markets/web-api/orders/#get-a-list-of-orders
                orders_df = pd.DataFrame([order._raw for order in orders])
                SPY_orders = [o for o in orders if o.symbol == 'SPY']
                open_sell_symbols = {order.symbol for order in orders if order.side == "sell" and order.status == "open" and order.filled_at == None}


                # with API (no SDK)
                    def order_list(status = "open", limit = 50):
                        params = {"status":status} 
                        r = requests.get(ord_url, headers=headers, params=params) # we do "get" as we want to receive info
                        data = r.json()
                        return pd.DataFrame(data)

                    order_df = order_list("closed") 
                

                # Get today's FILLs
                    start_datetime = pd.to_datetime('today').tz_localize('America/New_York').normalize()
                    todays_fills = alpaca.get_activities('FILL', date=start_datetime.isoformat())
                    todays_fills_df = pd.DataFrame([order._raw for order in todays_fills])

                    def confirm_full_execution(api, sym, qty, side, expected_qty):
                        sent = self.send_basic_order(sym, qty, side)
                        if(not sent):
                            return

                        executed = False
                        while(not executed):
                            try:
                                position = self.api.get_position(sym)
                                if int(position.qty) == int(expected_qty):
                                    executed = True
                                else:
                                    print(f"Waiting on execution for {sym}...")
                                    time.sleep(20)
                            except BaseException:
                                print(f"Waiting on execution for {sym}...")
                                time.sleep(20)
                        print(
                            f"Order of | {abs(qty)} {sym} {side} | completed.  Position is now {expected_qty} {sym}.")

                
                alpaca.get_order(order_id)
                # client-side unique order ID will be automatically generated by the system if not provided by the client
                my_order = alpaca.get_order_by_client_order_id('my_first_order')
                strategy_name = "Break_out_10min"
                coid = strategy_name + "_" + str(int(time.mktime(alpaca.get_clock().timestamp.timetuple())))


                alpaca.get_activities(after=datetime.date.today().isoformat())
                data = alpaca.get_activities() # deliveres only 100 rows
                data = pd.DataFrame([activity._raw for activity in data])
                alpaca.get_activities(activity_types=['FEE'], date='2021-12-28')
                # All activity types: https://alpaca.markets/docs/api-documentation/api-v2/account-activities/#activity-types


                df = alpaca.get_trades("AAPL", "2021-10-08", "2022-03-20", limit=10).df

                # get all trades - I put this rows as function to utils:
                                        # import utils_for_alpaca
                                        # utils_for_alpaca.get_trades(alpaca)
                        count = 0
                        search = True
                        while search:
                            if count < 1:
                                data = alpaca.get_activities()
                                data = pd.DataFrame([activity._raw for activity in data])
                                split_id = data.id.iloc[-1] # we need this as get_activities() delivers only 100 rows
                                trades = data
                            else:
                                data = alpaca.get_activities(direction='desc', page_token=split_id)
                                data = pd.DataFrame([activity._raw for activity in data])
                                if data.empty:
                                    search = False
                                else:
                                    split_id = data.id.iloc[-1]
                                    trades = trades.append(data)
                            count += 1

                        trades.groupby(["order_status", 'activity_type']).size()

                        # filter out partially filled orders
                        trades = trades[trades.order_status == 'filled'] 
                        trades = trades.reset_index(drop=True)
                        trades = trades.sort_index(ascending=False).reset_index(drop=True)
                        trades['transaction_time'] = pd.to_datetime(trades['transaction_time'], format="%Y-%m-%d") # convert filled_at to date
                        trades['transaction_time'] = trades['transaction_time'].dt.strftime("%Y-%m-%d") # remove time

                # get orders created today (if more than 500 orders)
                    # If you had an order created yesterday but it’s still open this won’t fetch it.
                    start_datetime = pd.to_datetime('today').tz_localize('America/New_York').normalize()
                    todays_orders = pd.DataFrame()
                    have_all_orders = False
                    while not have_all_orders:
                        order_chunk = alpaca.list_orders(status='all', after=start_datetime.isoformat())
                        order_chunk_df = pd.DataFrame([order._raw for order in order_chunk])
                        if not order_chunk_df.empty:
                            todays_orders = todays_orders.append(order_chunk_df)
                            start_datetime = pd.to_datetime(order_chunk_df.created_at.iat[-1]) # Get the last order created_time which becomes the start of the next chunk
                            have_all_orders = False
                        else:
                            have_all_orders = True


        # Define quantity which to buy
            current_asset_price = math.ceil(float(alpaca.get_latest_trade(symbol).p)) # round up is done with math.ceil()
            qty = int(abs(dollar_amount) / current_asset_price)


        # Get fees
            # the fees post the day after the trading
            # Since I'm trying to look at a daily P&L net of fees, ...
            # ... I need to adjust those fee dates back one trading day, ...
            # ... which gets a touch complicated with weekends and holidays
            account_fees = alpaca.get_activities(activity_types=['FEE'], date='2021-12-28')


        # Cancel / replace orders

                alpaca.cancel_order(order_id)
                alpaca.cancel_all_orders()

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


# Assets
    # 'shortable' = True, 'easy_to_borrow' = True, 'marginable' = True, asset_class=None
    # Alpaca currently uses its clearing firms ‘Easy To Borrow’ list and assumes everything else is Hard To Borrow. 
    # However there is no historical data for which stocks were hard to borrow earlier.

    assets = alpaca.list_assets(status="active")
    asset_df = pd.DataFrame([asset._raw for asset in assets if asset.tradable])
    my_asset_list = [position.asset_id for position in alpaca.list_positions()]
    my_asset_df = asset_df.query('id in @my_asset_list')


    nasdaq_assets = [a for a in assets if a.exchange == 'NASDAQ']
    amex_assets = [b for b in assets if b.exchange == 'AMEX']
    nyse_assets = [c for c in assets if c.exchange == 'NYSE']
    arca_assets = [d for d in assets if d.exchange == 'ARCA']


    alpaca.get_asset('MSFT').shortable*alpaca.get_asset('GME').easy_to_borrow
    alpaca.get_asset('MSFT').easy_to_borrow

    # there is get_snapshot() for 1 Ticker and get_snapshots() for many tickers
    # https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/historical/#snapshot---multiple-tickers
    x = alpaca.get_snapshot('MSFT')

    # Show top gainers:
        assets_list = alpaca.list_assets(status="active") # all stocks tradable on Aplaca
        active_asset_list = [asset.symbol for asset in assets_list if asset.status=='active'] # list of active symbols
        len(active_asset_list) # over 10k
        snapshots_dict = {}
        CHUNK_SIZE = 1000 # There is a maximum length a URI can be => so get the snapshots in 'chunks'
        for chunk_start in range(0, len(active_asset_list), CHUNK_SIZE):
            chunk_end = chunk_start + CHUNK_SIZE
            chunk = active_asset_list[chunk_start:chunk_end]
            snapshots_chunk = alpaca.get_snapshots(chunk)
            snapshots_dict.update(snapshots_chunk)

        snapshot_data = {stock: [snapshot.latest_trade.price, 
                                snapshot.prev_daily_bar.close,
                                snapshot.daily_bar.close,
                                (snapshot.daily_bar.close/snapshot.prev_daily_bar.close)-1,
                                ]
                        for stock, snapshot in snapshots_dict.items() if snapshot and snapshot.daily_bar and snapshot.prev_daily_bar
                        }

        snapshot_columns=['price', 'prev_close', 'last_close', 'gain']
        snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)
        top_gainers_over_3_dollars = snapshot_df.query('price>3').nlargest(10, 'gain')


# Position & portfolio

        # Current position

            positions = alpaca.list_positions()
            positions_df = pd.DataFrame([p._raw for p in positions])
            positions_df.columns
                '''
                'asset_id', 'symbol', 'exchange', 'asset_class', 'asset_marginable', 'qty', 
                'avg_entry_price', 'side', 'market_value', 'cost_basis',
                'unrealized_pl', 'unrealized_plpc', 'unrealized_intraday_pl',
                'unrealized_intraday_plpc', 'current_price', 'lastday_price','change_today', 'usd'
                '''
            positions = {p.symbol for p in positions()}
            position_qt =[]
            symbols = ['ADBE','SPY']
            position_qt = [p.qty for p in positions if p.symbol in symbols else 0]

            try:
                position = int(alpaca.get_position("AAPL").qty)
            except: # No position exists
                position = 0


            # get portfolio info
                data_pf = alpaca.get_portfolio_history(period='2W', # <number> + <unit>: D, W, M, A
                                                       timeframe='5Min', # 1Min, 5Min, 15Min, 1H, 1D
                                                       extended_hours = True
                                                       ).df.reset_index().iloc[0:-1,]
                data_pf['lagged_equity'] = data_pf.equity.shift(1)
                data_pf['pct_change'] = data_pf.profit_loss / data_pf.lagged_equity
                # filter by date if desired
                dateFilter = '2021-01-19'
                data = data_pf[data_pf.timestamp >= dateFilter].reset_index(drop=True)


            # via pure API (no SDK)
                def positions(symbol=""):
                    if len(symbol)>1:
                        r = requests.get(pos_url+"/{}".format(symbol), headers=headers)
                        return r.json() # no need to return a dataframe
                    else:
                        r = requests.get(pos_url, headers=headers)
                        return pd.DataFrame(r.json())

                positions()

            asset_list = alpaca.list_assets()
            asset_df = pd.DataFrame([asset._raw for asset in asset_list])
            my_asset_list = [position.asset_id for position in alpaca.list_positions()]
            my_asset_df = asset_df.query('id in @my_asset_list')


            positions_list = alpaca.list_positions()
            positions_dict = {position.symbol: position for position in positions_list}
            if symbol not in positions:
                current_price = alpaca.get_snapshot(symbol).minute_bar.close
                current_dollar_amt = 0.0
                current_qty = 0.0
            else:
                current_price = float(positions[symbol].current_price)
                current_dollar_amt = float(positions[symbol].market_value)
                current_qty = float(positions[symbol].qty)



            # check function get_current_positions() in utils

            from utils import *
            analyze_trades(alpaca, 'AAPL')


            def get_num_shares(stock):
                all_pos = alpaca.list_positions()
                i = 0
                for i in range(len(all_pos)):
                    if stock == all_pos[i].symbol:
                        return all_pos[i].qty
                return 0 # If we don't find it in all_pos


        # Close all positions

            alpaca.close_all_positions()
                      
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


            # Parallel closure
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
                portfolio = alpaca.list_positions()
                for position in portfolio:
                    profit = position.unrealized_pl
                    percentChange = (profit/position.cost_basis) * 100
                    if (percentChange > 5):
                        print("Selling {} shares of {}".format(position.qty,position.symbol))
                        alpaca.submit_order(symbol=position.symbol,qty=position.qty,side='sell',type='market',time_in_force='opg')


            # Sell everything a minute after the market opens
                clock = alpaca.get_clock()
                time_after_open = clock.next_open - clock.timestamp
                if time_after_open.seconds >= 60:
                    print('Liquidating positions.')
                    alpaca.close_all_positions()


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

        # For the Unlimited plan, we receive direct feeds from the CTA (administered by NYSE) and UTP (administered by Nasdaq) SIPs. 
        #   These 2 feeds combined offer 100% market volume.
        #   For more information about market data feeds: https://medium.com/automation-generation/exploring-the-differences-between-u-s-stock-market-data-feeds-3da26946cbd6

        # List of exchange codes: https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/#exchanges
        # List of trade & quote conditions from 2 SIPs: https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/#conditions

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


        end_date = pd.Timestamp('now').date().isoformat()
        end_date = dt.datetime.now().strftime("%Y-%m-%d")
        end_dt = alpaca.get_clock().timestamp.date().isoformat()
        days = 200
        n_days_ago = (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%d")

        AAPL = alpaca.get_bars("AAPL", TimeFrame.Minute, "2015-12-01", end_dt, limit=300, adjustment='all').df # 'dividend', 'split', 'all'
                                        # TimeFrame.Day, '1Day'

        raw_bars = alpaca.get_bars(Universes.TOP10_US_SECTOR, TimeFrame.Minute, end_date, end_date, adjustment='all').df
        raw_bars.index = raw_bars.index.tz_convert('America/New_York').tz_localize(None) # Convert to market time for easier reading


        # special length bars (e.g. 5Min or 20 Min)
            from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit # this is need to setup special length bars (e.g. 5Min or 20 Min)
                                # However, this Class is available only in v 1.4.1, so I did like this:
                                #       1. Unistall existing alpaca SDK
                                #       2. Install needed version: pip install alpaca-trade-api==1.4.1 --ignore-installed PyYAML
                                #                                                  ingnore is needed as there was some problem with PyYAML
            minute_frame = 10 # means 1 day is 39 rows => 43k rows for 5 years 
            historicalData = {}
            for symbol in tickers:
                temp = alpaca.get_bars(symbol, TimeFrame(minute_frame, TimeFrameUnit.Minute), n_days_ago, end_date, adjustment='raw').df
                temp.between_time('09:31', '16:00') # focus on market hours as for now trading on alpaca is restricted to market hours
                temp.index = temp.index.tz_localize(None) # remove +00:00 from datetime
                historicalData[symbol]=temp

        # get minute data from 2015 (1 csv - 1 ticker) + summary

            import Universes
            # column_names = ['symbol','start','end','num_rows','completed']
            # data_summary_df = pd.DataFrame(columns = column_names)
            data_summary_df = pd.read_excel('Alpaca_minute_quotes_overview.xlsx',index_col=0) # checking current status
            need_symbols = Universes.hist_index_member
            need_symbols = Universes.TOP10_US_SECTOR
            done_symbols=data_summary_df.symbol.to_list() # already downloaded symbols
            still_missing = list(set(need_symbols) - set(done_symbols))
            Alpaca_directory = 'D:\\Data\\minute_data\\US\\alpaca_ET_adj\\'
            for idx, symbol in enumerate(still_missing):
                try:
                    temp = alpaca.get_bars(symbol, TimeFrame.Minute, "2015-12-01", "2022-05-20", adjustment='all').df
                    temp.index = temp.index.tz_convert('America/New_York') # Convert to market time for easier reading
                    temp.index = temp.index.tz_localize(None) # remove +00:00 from datetime
                    nameoffile=Alpaca_directory+symbol+"_ET_adj_alpaca.csv"
                    temp.to_csv(nameoffile)
                    data_summary_df.loc[len(done_symbols)+idx+1] = [symbol, temp.index[0], temp.index[-1],len(temp)]
                except:
                    print('Failure with {}'.format(symbol))
                    data_summary_df.loc[len(done_symbols)+idx+1] = [symbol, "no", "no",0]
                    pass
            data_summary_df.to_excel('Alpaca_minute_quotes_overview.xlsx')

            Tickers=[]
            Tickers.append([x.split('_ET_')[0] for x in os.listdir(Alpaca_directory) if x.endswith(".csv")])
            Tickers[1]




        # async modules for get_bars
            # you could use the new async modules for get_bars
            # https://github.com/alpacahq/alpaca-trade-api-python/blob/master/examples/historic_async.py
            # The problem with using threads is that it generates too many requests to the server. 
            #           Say I would like to get the last bar of 1000 symbols, 
            #           It would be less stressfull to request 1000 symbols directly, instead of asking one by one. 

        # Threads: ranks all stocks by percent change over the past 10 minutes (higher is better).
            import threading
            stockUniverse = ['DOMO', 'TLRY', 'SQ', 'MRO', 'AAPL', 'GM', 'SNAP', 'SHOP']
            period_length = 10
            allStocks = []
            for stock in stockUniverse:
                allStocks.append([stock, 0])

            def getPercentChanges():
                length = 10
                for i, stock in enumerate(allStocks):
                    bars = alpaca.get_bars(stock[0], TimeFrame.Day, pd.Timestamp('now').date()- dt.timedelta(days=period_length),pd.Timestamp('now').date(), limit=length,adjustment='raw')
                    allStocks[i][1] = (bars[len(bars) - 1].c - bars[0].o) / bars[0].o

            def rank():
                tGetPC = threading.Thread(target=getPercentChanges)
                tGetPC.start()
                tGetPC.join()

            rank()

        # Last trade and quote
            # Replicate v2 from v1 implementation
            # Check here: https://alpaca.markets/learn/latest-trade-quote/

            symbols = ['AAPL','SPY','F','NVDA']
            alpaca.get_latest_quote("AAPL")
            alpaca.get_latest_trade("AAPL")

            current_asset_price = math.ceil(float(alpaca.get_latest_trade(ticker).p)) # round up is done with math.ceil()


            bars = alpaca.get_latest_bars(symbols)
            trades = alpaca.get_latest_trades(symbols)
            quotes = alpaca.get_latest_quotes(symbols)


            alpaca.get_quotes("AAPL", "2021-02-08", "2021-02-08", limit=10).df

        # Overnight & intraday gain
            import math
            stock = 'SPY'
            snapshot_data = alpaca.get_snapshot(stock)
            dir(snapshot_data)
            price_current = snapshot_data.minute_bar.close
            price_prev_close = snapshot_data.prev_daily_bar.close
            price_open = snapshot_data.prev_daily_bar.open
            gain_close_to_current = math.log(price_current / price_prev_close)
            gain_open_to_current = math.log(price_current / price_open)

        # Calculate n day volatility of intra_day gains
            import numpy as np
            stock = 'SPY'
            PREV_STD_DAYS = 4
            today = alpaca.get_clock().timestamp.date()
            previous_day = today - pd.Timedelta('1D')
            previous_day_10 = today - pd.Timedelta('10D')
            bar_data = alpaca.get_bars(stock, TimeFrame.Day, previous_day_10.isoformat(), previous_day.isoformat(), 'raw' ).df
            bar_data['intra_day_gain'] = np.log(bar_data.close/bar_data.open)
            bar_data['volatility'] = bar_data.rolling(PREV_STD_DAYS).intra_day_gain.std()
            volatility = bar_data.volatility[-1]

        # Crypto
            btc = alpaca.get_crypto_bars("BTCUSD", TimeFrame.Minute, "2021-08-01", "2021-11-01").df

            btc = btc[btc['exchange'] == 'CBSE']
            coin = alpaca.get_bars("COIN", TimeFrame.Minute, "2021-08-01","2021-11-01").df
            btc['BTC_minutely_return'] = btc['close'].pct_change()
            coin['COIN_minutely_return'] = coin['close'].pct_change()

            bar_data = alpaca.get_crypto_bars('BTCUSD', TimeFrame.Hour, "2021-06-08", "2021-06-09").df
            trade_data = alpaca.get_crypto_trades('BTCUSD', "2021-06-08", "2021-06-09").df # gets trade data for Bitcoin
            quote_data = alpaca.get_crypto_quotes('BTCUSD',"2021-06-08", "2021-06-09").df # gets quote data for Bitcoin


# Working with time and dates, market hours

    current_day = alpaca.get_clock().timestamp.date().isoformat()

    today= dt.date.today().isoformat()
    treedaysago=(dt.date.today() - dt.date.timedelta(days= -3)).isoformat()

    dt.datetime.now().strftime("%Y-%m-%d")
    end_dt = alpaca.get_clock().timestamp.date()

    days = 1000
    today = dt.datetime.now().strftime("%Y-%m-%d")
    n_days_ago = (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    stock1_barset = alpaca.get_bars(stock1, TimeFrame.Day,n_days_ago,today,adjustment='raw').df

    alpaca.get_calendar("2021-02-08", "2021-02-18") # start=None, end=None

    now = pd.Timestamp.now(tz='America/New_York').floor('1min')
    today = now.strftime('%Y-%m-%d')
    tomorrow = (now + pd.Timedelta('1day')).strftime('%Y-%m-%d')

    today = alpaca.get_clock().timestamp.date()
    previous_day = today - pd.Timedelta('1D')
    previous_day_10 = today - pd.Timedelta('10D')
    bar_data = alpaca.get_bars(stock, TimeFrame.Day, previous_day_10.isoformat(), previous_day.isoformat(), 'raw' ).df


    trades.index = trades.index.tz_convert('America/New_York')
    raw_bars.index = raw_bars.index.tz_convert('America/New_York') # Convert to market time for easier reading
    temp.index = temp.index.tz_localize(None) # remove +00:00 from datetime


    # get pre-market trades for several days
        symbols = ['IBM', 'F']
        start = pd.to_datetime('2021-01-01').tz_localize('America/New_York').tz_convert('America/New_York').normalize()
        end = pd.to_datetime('2021-01-05').tz_localize('America/New_York').tz_convert('America/New_York').normalize()
        trading_calendars = api_data.get_calendar(start_date_et.isoformat(), end_date_et.isoformat())
        trading_days = [pd.to_datetime(calendar.date).tz_localize('America/New_York') for calendar in trading_calendars]
        pre_market_trades = pd.DataFrame()
        for day in trading_days:
            begin_pre_market = day + pd.Timedelta('9:00:00')
            end_pre_market = day + pd.Timedelta('9:29:00')
            day_trades = api_data.get_trades(symbols, begin_pre_market.isoformat(), end_pre_market.isoformat()).df
            pre_market_trades = pre_market_trades.append(day_trades)



    # Wait till market opens
        clock = alpaca.get_clock()
        closing = clock.next_close - clock.timestamp
        if round(closing.total_seconds() / 60) > 120:
            if not clock.is_open:
                time_to_open = (clock.next_open - clock.timestamp).total_seconds()
                print(f"Market is closed now going to sleep for ~{time_to_open.total_seconds()//3600} hours till {clock.next_open.ctime()}")
                sleep(round(time_to_open))
    		
    # Buy couple minutes before market close
        clock = alpaca.get_clock()
        if clock.is_open:
            time_until_close = clock.next_close - clock.timestamp # Wait to buy
            if time_until_close.seconds <= 120:
                print('Buying positions...')
                # or close all positions
                # ...
                print('Positions bought.')
            print("Sleeping until market close (15 minutes).")
            time_to_open = (clock.next_open - clock.timestamp).total_seconds()
            sleep(round(time_to_open))

    # Sell everything a minute after the market opens
        clock = alpaca.get_clock()
        time_after_open = clock.next_open - clock.timestamp
        if time_after_open.seconds >= 60:
            print('Liquidating positions.')
            alpaca.close_all_positions()

    # Check if data is current
        clock = alpaca.get_clock()
        current_time = clock.timestamp
        current_date = clock.timestamp.normalize()
        minute_bar_is_old = snapshot_data.minute_bar.timestamp < current_time - pd.Timedelta(15, 'minutes')
        daily_bar_is_old = snapshot_data.daily_bar.timestamp < current_date

    # Calculate X periods volatility of intra_day gains
        import numpy as np
        stock = 'SPY'
        PREV_STD_DAYS = 4
        today = alpaca.get_clock().timestamp.date()
        previous_day = today - pd.Timedelta('1D')
        previous_day_10 = today - pd.Timedelta('10D')
        bar_data = alpaca.get_bars(stock, TimeFrame.Day, previous_day_10.isoformat(), previous_day.isoformat(), 'raw' ).df
        bar_data['intra_day_gain'] = np.log(bar_data.close/bar_data.open)
        bar_data['volatility'] = bar_data.rolling(PREV_STD_DAYS).intra_day_gain.std()
        volatility = bar_data.volatility[-1]

    # get last X trading days
        days_of_interest = 10
        current_day = alpaca.get_clock().timestamp.date().isoformat()
        trading_days = alpaca.get_calendar()
        trading_days_df = pd.DataFrame([day._raw for day in trading_days])
        my_dates = trading_days_df.query('date < @current_day').tail(days_of_interest)

    # split day into time buckets
        test_Data.loc[test_Data.between_time('04:00:00', '7:59:00').index, 'Time_Slot'] = 'Early_pre_market'
        test_Data.loc[test_Data.between_time('08:00:00', '9:28:00').index, 'Time_Slot'] = 'Immidiate_pre_market'
        test_Data.loc[test_Data.between_time('09:29:00', '9:31:00').index, 'Time_Slot'] = 'Openning'
        test_Data.loc[test_Data.between_time('09:32:00', '15:49:00').index, 'Time_Slot'] = 'Trading'
        test_Data.loc[test_Data.between_time('15:50:00', '15:59:00').index, 'Time_Slot'] = 'Closing'
        test_Data.loc[test_Data.between_time('16:00:00', '16:59:00').index, 'Time_Slot'] = 'Immidiate_after_market'
        test_Data.loc[test_Data.between_time('17:00:00', '19:59:00').index, 'Time_Slot'] = 'Late_after_market'
        test_Data.loc[test_Data.between_time('17:00:00', '19:59:00').index, 'Time_Slot'] = 'Late_after_market'

    # tagging historical last and 1st trading day of a month
        trading_days_df = pd.DataFrame([day._raw for day in alpaca.get_calendar("2014-12-01", "2022-05-23")])
        trading_days_df['date'] = pd.to_datetime(trading_days_df['date'], format='%Y-%m-%d')
        trading_days_df['DOM'] = np.where(trading_days_df.date.dt.to_period('M') != trading_days_df.date.shift().dt.to_period('M'), 'FDM', 
                                 np.where(trading_days_df.date.dt.to_period('M') != trading_days_df.date.shift(-1).dt.to_period('M'), 'EOM', 'No'))   # FDM - 1st day of month, EOM - end of Month
        trading_days_df = trading_days_df.set_index('date')
        trading_days_df['Weekday_Name'] = trading_days_df.index.dayofweek # 0 = Monday
        trading_days_df[19:24]
        trading_days_df[(trading_days_df.session_close=='1600') | (trading_days_df.close=='1300')] # there are some closes at 13:00


# News
    alpaca.get_news()
    alpaca.get_news(['AAPL','AMZN']) # default - 10 latest
    news = alpaca.get_news("AAPL", "2021-01-01", "2021-12-31")