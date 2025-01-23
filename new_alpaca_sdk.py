# New SDK: https://github.com/alpacahq/alpaca-py

import Universes

import pandas as pd
import time
import os
import datetime as dt
from dateutil.relativedelta import relativedelta
import random

import alpaca
alpaca.__version__

from alpaca.common.enums import Sort
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderStatus, OrderType, OrderClass, QueryOrderStatus, CorporateActionType, CorporateActionSubType
from alpaca.trading.requests import GetCalendarRequest, GetAssetsRequest, GetOrdersRequest, MarketOrderRequest, LimitOrderRequest, StopLossRequest, TakeProfitRequest, TrailingStopOrderRequest, ReplaceOrderRequest, GetPortfolioHistoryRequest, GetCorporateAnnouncementsRequest
from alpaca.data.requests import StockLatestQuoteRequest, StockTradesRequest, StockLatestTradeRequest, StockQuotesRequest, StockBarsRequest, StockSnapshotRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed, Exchange


from alpaca.trading.client import TradingClient
from alpaca.data import StockHistoricalDataClient
from alpaca.broker.client import BrokerClient


API_KEY_PAPER = os.environ['ALPACA_API_KEY_PAPER']
API_SECRET_PAPER = os.environ['ALPACA_API_SECRET_PAPER']

stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)
broker_client = BrokerClient(API_KEY_PAPER,API_SECRET_PAPER,sandbox=False,api_version="v2")
trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER) # dir(trading_client)

# to more easily convert the portfolio to a dataframe, instantiate the TradingClient with raw_data=True
stock_client_df = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER, raw_data=True)
trading_client_df = TradingClient(API_KEY_PAPER, API_SECRET_PAPER, paper=True, raw_data=True)


# Account information

        # Portfolio history

            # 'GetPortfolioHistoryRequest' is only available in 'broker_client', So we use get() method in 'trading_client'
            # https://docs.alpaca.markets/reference/getaccountportfoliohistory

            endpoint = '/account/portfolio/history'
            params = {'timeframe':'1H', # for >30 days only 1D, otherwise 1Min, 5Min, 15Min, 1H
                    'period': '1D', # <number> + <unit>: D, W, M, A
                    # 'date_start':'2023-11-30',
                    # 'date_end':'2023-12-20',
                    'intraday_reporting':'continuous',
                    }
            history = pd.DataFrame(trading_client.get(path=endpoint, data=params))
            history['lagged_equity'] = history.equity.shift(1)
            history['pct_change'] = history.profit_loss / history.lagged_equity



# Submit orders
        

        strategy_name = "Testing stop-loss"
        coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
        ticker = 'SPY'
        ticker = random.choice(Universes.Spiders)
        limit_buy = 0.99 # how limit price should be different from current ask
        stop_loss = trail_sl = 5 # in %, easy stop-loss, I should use ATR() like in zorro probably
        take_profit = 20 # in %        
        account = trading_client.get_account()
        latest_quote = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[ticker]))
        limit_price_target = round(latest_quote[ticker].ask_price*limit_buy,0)
        stop_loss_price = round(limit_price_target*(1-stop_loss/100),0)
        quantity = int(float(account.buying_power)//latest_quote[ticker].ask_price)
        quantity = 2

        # Market order
        market_order_data = MarketOrderRequest(
            symbol=ticker,
            qty=2,
            side=OrderSide.BUY, # dir(OrderSide)
            # extended_hours = True, # not possible for market order
            client_order_id = coid,
            order_class = OrderClass.OTO, # this is needed to "attach" the stop-loss; dir(OrderClass) - ['BRACKET', 'OCO', 'OTO', 'SIMPLE']
            stop_loss = StopLossRequest(stop_price=stop_loss_price),
            time_in_force=TimeInForce.GTC) # dir(TimeInForce)
        market_order = trading_client.submit_order(order_data=market_order_data)
        print(f"Buying {quantity} of {ticker}. Buing power left is {float(account.buying_power)}")
        status = None
        while status != 'filled':
            my_order = trading_client.get_order_by_id(market_order.id) # get_order_by_client_id()
            status = my_order.status
                # dir(OrderStatus) # 'ACCEPTED', 'ACCEPTED_FOR_BIDDING', 'CALCULATED', 'CANCELED', 
                                # 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED', 'REPLACED', 'FILLED', 'NEW', 'PARTIALLY_FILLED', 
                                # 'PENDING_CANCEL', 'PENDING_NEW', 'PENDING_REPLACE',  'STOPPED', 'SUSPENDED'
                # full list of statuses: https://alpaca.markets/docs/trading-on-alpaca/orders/#order-lifecycle
                # Updates on open orders at Alpaca will also be sent over the streaming interface, ...
                # ... which is the recommended method of maintaining order state.
                    # https://docs.alpaca.markets/docs/working-with-orders#listen-for-updates-to-orders

        




        # bracket order
        strategy_name = "Testing bracket order"
        coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
        ticker = random.choice(Universes.Spiders)
        limit_buy = 0.99 # how limit price should be different from current ask
        stop_loss = trail_sl = 5 # in %, easy stop-loss, I should use ATR() like in zorro probably
        take_profit = 20 # in %        
        account = trading_client.get_account()
        latest_quote = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[ticker]))
        limit_price_target = round(latest_quote[ticker].ask_price*limit_buy,0)
        stop_loss_price = round(limit_price_target*(1-stop_loss/100),0)
        take_profit_price = round(limit_price_target*(1+take_profit/100),0)
        quantity = int(float(account.buying_power)//latest_quote[ticker].ask_price)
        quantity = 2
        bracket_market_order_data = MarketOrderRequest(
            symbol = ticker, 
            qty = quantity, 
            side = OrderSide.BUY, 
            time_in_force = TimeInForce.GTC, 
            take_profit = TakeProfitRequest(limit_price=take_profit_price, side = OrderSide.SELL), 
            stop_loss= StopLossRequest(stop_price=stop_loss_price, side = OrderSide.SELL), 
            order_class = OrderClass.BRACKET
            )
        bracket_order = trading_client.submit_order(order_data=bracket_market_order_data)
        status = None
        while status != 'filled':
            my_order = trading_client.get_order_by_id(bracket_order.id) # get_order_by_client_id()
            status = my_order.status


        # Trailing order: https://alpaca.markets/docs/trading/orders/#trailing-stop-orders
        trailing_sl_order_data = TrailingStopOrderRequest(
            symbol=ticker,
            qty=quantity,
            side=OrderSide.BUY, # dir(OrderSide)
            # extended_hours = True, # not possible for traling order
            client_order_id = 'testing trailing order 3',
            trail_percent=trail_sl,
            time_in_force=TimeInForce.GTC) # dir(TimeInForce)
        trailing_order = trading_client.submit_order(order_data=trailing_sl_order_data)


        # Limit order
        def limit_buy_order(self, seconds_toCancel=30):
            seconds_toCancel = 30
            strategy_name = "Testing limit order"
            coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
            ticker = random.choice(Universes.Spiders)
            limit_buy = 0.99 # how limit price should be different from current ask
            stop_loss = trail_sl = 5 # in %, easy stop-loss, I should use ATR() like in zorro probably
            take_profit = 20 # in %        
            account = trading_client.get_account()
            latest_quote = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[ticker]))
            limit_price_target = round(latest_quote[ticker].ask_price*limit_buy,0)
            stop_loss_price = round(limit_price_target*(1-stop_loss/100),0)
            try:
                limit_order_data = LimitOrderRequest(
                    symbol=ticker,
                    limit_price=limit_price_target,
                    qty=300,
                    side=OrderSide.BUY,
                    extended_hours = True,
                    client_order_id = coid,
                    # order_class = OrderClass.OTO, # this is needed to "attach" the stop-loss
                    # stop_loss = StopLossRequest(stop_price=stop_loss_price),
                    time_in_force=TimeInForce.DAY) # dir(TimeInForce), extended hours order must be DAY limit orders
                limit_order = trading_client.submit_order(order_data=limit_order_data)
                logging.info(f'Placed order for {quantity} shares at ${limit_price_target:.2}...')
                logging.info('Waiting for order to fill...')

                # orders that satisfy params
                order_params = GetOrdersRequest(status=QueryOrderStatus.OPEN, side=OrderSide.BUY)
                start_time = time.time()
                while len(trading_client.get_orders(filter=order_params)) > 0:
                    # Cancel order if it takes too long to fill
                    if time.time() - start_time > seconds_toCancel:
                        logging.warning('Order took too long to fill. Canceling order...')
                        trading_client.cancel_order_by_id(limit_order.id)
                        break
                    time.sleep(3)
                logging.info('Order filled!')
                return limit_order.id
            except Exception as e:
                logging.error(f'ERROR: Attempted order for {quantity} shares at ${limit_price_target:.2}')
                logging.error(e)
                limit_order.created_at
                return None


        # placing a stop limit order would always ‘hide’ the order until the stop is triggered
        # https://docs.alpaca.markets/docs/orders-at-alpaca#stop-limit-order
        # 1) The stop-limit order will be executed after a given stop price has been reached.
        # 2) The stop-limit order becomes a limit order to buy or sell at the limit price or better.
        # 3) Specify both the limit and stop price parameters




        # replace existing limit order: trading_client.replace_order_by_id()
        ticker = 'SPY'
        existing_order = limit_order        
        latest_quote = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[ticker]))
        limit_price = round(latest_quote[ticker].ask_price*limit_buy,0)-3
        if existing_order:
            if float(existing_order.limit_price) == float(limit_price):
                logging.debug(f"{ticker}: place_trade: Replacement limit price {limit_price} is same as existing order limit price {existing_order.limit_price}. Skipping...")
                return
            try:
                logging.debug(f"{ticker}: place_trade: Replacing existing order limit price {existing_order.limit_price} with {limit_price}")
                order_replace_data = ReplaceOrderRequest(limit_price=limit_price)
                trading_client.replace_order_by_id(existing_order.id, order_data=order_replace_data)
            except Exception as e:
                logging.warning(f"{ticker}: place_trade: Error while replacing the order for {ticker}: {e}")



        # close position
        trading_client.close_position('XLE') 
        trading_client.close_all_positions(cancel_orders=True) # closes all position AND also cancels all open orders

        # Take profit
        positions = trading_client.get_all_positions()
        strategy_name = "Closing_big_profits"
        for position in positions:
            if (float(position.unrealized_plpc) > 0.1):
                try:
                    print(f"Selling {position.qty} shares of {position.symbol} as profit is high: {round(100*float(position.unrealized_plpc),0)}%")
                    coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
                    trading_client.submit_order(MarketOrderRequest(symbol=position.symbol,qty=position.qty,side=OrderSide.SELL,client_order_id = coid,time_in_force=TimeInForce.GTC))
                except:
                    pass



        # cancel orders
        trading_client.cancel_orders() 
        trading_client.cancel_order_by_id()

        '''
        How to switch from long to short? 
        2 orders are required: 
            (Option A) Send 1st order. Listen on web socket for order ack. Then send 2nd order. 
            (Option B) Send 1st order. Sleep for some configurable amount of time. Then send 2nd order. Retry 2nd order if rejected for ‘invalid qty’. 
        '''






# Clock & Calendar

    alpaca_calendar = trading_client.get_calendar()
    alpaca_calendar = trading_client.get_calendar(GetCalendarRequest(start="2021-02-08", end="2021-02-18"))
    len(alpaca_calendar)
    alpaca_calendar[-1]


    # Check if it is first or last trading day in the month

        result = {
            "market_status": "closed",
            "trading_day": "normal",
            "time_since_market_close": None,
            "time_till_market_open": None,
            "time_since_market_open": None,
            "time_till_market_close": None,
            "trading_days_past": None,
            "trading_days_future": None,
            
        }

        clock = trading_client.get_clock()
        current_time = clock.timestamp
        current_date = current_time.date()

        if clock.is_open:
            result["market_status"] = "open"
            open_time = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
            result["time_since_market_open"] = round((current_time - open_time).total_seconds() / 60,1)
            result["time_till_market_close"] = round((clock.next_close - current_time).total_seconds() / 60,1)
        else:
            calendar = trading_client.get_calendar(GetCalendarRequest(start=(current_date + pd.DateOffset(days=-5)), end=current_date))   
            result["time_since_market_close"] = round((current_time.replace(tzinfo=None) - calendar[-1].close).total_seconds() / 60,1) # calendar[-1] - latest trading day: today or earlier
            result["time_till_market_open"] = round((clock.next_open - current_time).total_seconds() / 60,1)


        first_of_month = current_date.replace(day=1)
        last_of_month = (current_date.replace(day=1) + pd.DateOffset(months=1)+pd.DateOffset(days=-1))
        calendar = trading_client.get_calendar(GetCalendarRequest(start=first_of_month, end=last_of_month))
        trading_days = [day.date for day in calendar]
        if current_date not in trading_days: # if today is not in trading days, ...
            result["trading_day"] = "off" # ... today is not a trading day
        elif calendar[0].date == current_date: # first trading day in month
            result["trading_day"] = "first"
        elif calendar[-1].date == current_date: # last trading day in month
            result["trading_day"] = "last"

        trading_days_future = len([day for day in trading_days if day > current_date])
        trading_days_past = len([day for day in trading_days if day < current_date])
        result["trading_days_past"] = trading_days_past
        result["trading_days_future"] = trading_days_future















    now = pd.Timestamp.today() + pd.offsets.Day(-1)
    MonthEnd = (now + pd.offsets.BusinessMonthEnd(normalize=True)).strftime("%Y-%m-%d")
    trading_till_moe = trading_client.get_calendar(GetCalendarRequest(start=now.strftime("%Y-%m-%d"), end=MonthEnd))
    len(trading_till_moe)

    weird_close_times = [day for day in trading_till_moe if day.close.hour != 16] # check if in the upcoming days exchange closes earlier than usual

    pd.Timestamp(trading_till_moe[0].close).strftime("%b %d, %H:%M") # Close time ET today 
    pd.Timestamp(trading_till_moe[0].close).tz_localize('US/Eastern').tz_convert('UTC')

    clock = trading_client.get_clock()
    clock.is_open
    clock.timestamp.date()

    print(f'Today is {pd.Timestamp.today().day_name()}, {pd.Timestamp.now(tz="CET").tz_localize(None).strftime("%b %d, %H:%M") } in Munich, which is {pd.Timestamp.now(tz="EST").tz_localize(None).strftime("%H:%M")} in New York')
    time_to_open = (clock.next_open - clock.timestamp).total_seconds()//3600
    print(f'Market is currently closed. Will open in {time_to_open} hours')
    time_to_close = (clock.timestamp - clock.next_close).total_seconds()//3600
    print(f'Market is currently open. Will close in {time_to_close} hours')

    # if market not open, exit
    if not clock.is_open:
        time_to_open = (clock.next_open - clock.timestamp).total_seconds()//3600
        print(f'Market is currently closed. Will open in {time_to_open} hours at {clock.next_open.ctime()}')
        exit()
    else:
        time_to_close = (clock.next_close - clock.timestamp).total_seconds()//60


    clock.timestamp.strftime('%H:%M') > '15:00'
    # if it's close to market close time, then ...
    if (clock.timestamp.strftime('%H:%M') > '9:35' or clock.timestamp.strftime('%H:%M') < '15:49'):



# Market data stocks ----------------------------------------------------------------------------------------------------------------

        # For the Unlimited plan, we receive direct feeds from the CTA (administered by NYSE) and UTP (administered by Nasdaq) SIPs. 
        #   These 2 feeds combined offer 100% market volume.
        #   For more information about market data feeds: https://medium.com/automation-generation/exploring-the-differences-between-u-s-stock-market-data-feeds-3da26946cbd6

        # List of exchange codes: https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/#exchanges
        # List of trade & quote conditions from 2 SIPs: https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/#conditions




        # Quotes

            # to more easily convert the portfolio to a dataframe, instantiate the TradingClient with raw_data=True
            stock_client_df = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER, raw_data=True)

            quotes_request_params = StockQuotesRequest(
                symbol_or_symbols=["SPY"], 
                start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(60, "hours")).floor(freq='S'), # T - for minutes, H - for hours
                end = pd.Timestamp.now(tz="US/Eastern"), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
                limit = 100, # upper limit of number of data points
                feed = DataFeed.SIP
                )
            latest_multisymbol_quotes = stock_client_df.get_stock_quotes(quotes_request_params)
            latest_multisymbol_quotes["GLD"]['ap'] # ask_exchange, ask_price, ask_size, bid_exchange, bid_price, bid_size, conditions, tape, timestamp
            pd.DataFrame(latest_multisymbol_quotes["GLD"])


            latest_multisymbol_quotes = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=["SPY", "GLD", "TLT"],feed=DataFeed.SIP))
            latest_multisymbol_quotes["GLD"]['ap'] # ask_exchange, ask_price, ask_size, bid_exchange, bid_price, bid_size, conditions, tape, timestamp
            pd.DataFrame(latest_multisymbol_quotes)


            # Stream quotes
                from alpaca.data import StockDataStream
                # from alpaca.data.live import StockDataStream

                stock_stream = StockDataStream(API_KEY_PAPER,API_SECRET_PAPER)
                wss_client = StockDataStream(API_KEY_PAPER,API_SECRET_PAPER)
                async def quote_data_handler(data: Any):
                    # quote data will arrive here
                    print(data)
                wss_client.subscribe_quotes(quote_data_handler, "SPY")
                wss_client.run()


                stocks = ('SPY', 'SQQQ',)
                wss_client.subscribe_quotes(handle_quotes, *stocks)
                wss_client.subscribe_bars(handle_bars, *stocks)
                wss_client.run()

                # get some fake data to make sure your code works, even when the market is closed
                # wss://stream.data.alpaca.markets/v2/test


        # Trades
            trades_request_params = StockTradesRequest(
                symbol_or_symbols=["SPY","NVDA","F"], 
                start = pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(60, "days"),
                end = pd.Timestamp.now(tz="US/Eastern"), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
                limit = 10, # upper limit of number of data points
                feed = DataFeed.SIP
                )
            latest_multisymbol_trades = stock_client.get_stock_trades(trades_request_params).data
            flattened_data = []
            for symbol, trades in latest_multisymbol_trades.items():
                for trade in trades:
                    # Convert each Trade object to a dictionary
                    trade_dict = {
                        'conditions': trade.conditions,
                        'exchange': trade.exchange,
                        'id': trade.id,
                        'price': trade.price,
                        'size': trade.size,
                        'symbol': trade.symbol,
                        'tape': trade.tape,
                        'timestamp': trade.timestamp
                    }
                    flattened_data.append(trade_dict)

            # Create DataFrame
            df = pd.DataFrame(flattened_data)




            trades_request_params = StockLatestTradeRequest(symbol_or_symbols=["SPY","NVDA","F"], feed = DataFeed.SIP)
            latest_multisymbol_trades = stock_client.get_stock_latest_trade(trades_request_params)



            # Streaming Trade Updates
                from alpaca.trading.stream import TradingStream
                trading_stream = TradingStream(API_KEY_PAPER,API_SECRET_PAPER, paper=True)
                async def update_handler(data):     # trade updates will arrive in our async handle
                    print(data)
                trading_stream.subscribe_trade_updates(update_handler)
                trading_stream.run()


        # Market movers
            # https://docs.alpaca.markets/reference/mostactives
            # https://docs.alpaca.markets/reference/movers
            # No SDK so far (15.10.2023)


# Crypto data -------------------------------------------------------------------------------------------------------------------------
    from alpaca.data import CryptoHistoricalDataClient, StockHistoricalDataClient
    from alpaca.data.historical import CryptoHistoricalDataClient
    from alpaca.data.requests import CryptoBarsRequest
    from alpaca.data.timeframe import TimeFrame
    from alpaca.trading.requests import GetAssetsRequest
    from alpaca.trading.enums import AssetClass


    client = CryptoHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER) # no keys required for crypto data
    request_params = CryptoBarsRequest(symbol_or_symbols=["BTC/USD", "ETH/USD"],timeframe=TimeFrame.Day,start="2022-07-01")
    bars = client.get_crypto_bars(request_params).df

    from alpaca.data import CryptoDataStream
    crypto_stream = CryptoDataStream(API_KEY_PAPER,API_SECRET_PAPER) # keys are required for live data

    # search for crypto assets
    assets = trading_client.get_all_assets(GetAssetsRequest(asset_class=AssetClass.CRYPTO))




positions = trading_client.get_all_positions()
positions_symbols = [p.symbol for p in positions]
date_filter = (pd.Timestamp.now()- pd.Timedelta(90, "days")).floor(freq='S') # orders for last 30 days
request_params = GetOrdersRequest(status=QueryOrderStatus.CLOSED, # Not the same as OrderStatus
                                # after = date_filter,
                                # side=OrderSide.BUY, # optional
                                symbols = positions_symbols
                                )
orders = trading_client.get_orders(filter=request_params)
orders_dicts = []
for entry in orders:
    entry_dict = {}  # Create a new dictionary for each entry
    for attribute, value in vars(entry).items():
        entry_dict[attribute] = value  # Populate the dictionary
    orders_dicts.append(entry_dict)  # Append the populated dictionary to the list
df = pd.DataFrame(orders_dicts)
for column in df.columns:
    if pd.api.types.is_datetime64tz_dtype(df[column]):
        df[column] = df[column].dt.tz_localize(None)


df[['filled_at','status','side']]
