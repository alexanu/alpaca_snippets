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


API_KEY_PAPER = os.environ['API_KEY_PAPER']
API_SECRET_PAPER = os.environ['API_SECRET_PAPER']

stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)
broker_client = BrokerClient(API_KEY_PAPER,API_SECRET_PAPER,sandbox=False,api_version="v2")
trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER) # dir(trading_client)

# to more easily convert the portfolio to a dataframe, instantiate the TradingClient with raw_data=True
stock_client_df = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER, raw_data=True)
trading_client_df = TradingClient(API_KEY_PAPER, API_SECRET_PAPER, paper=True, raw_data=True)




# Account information

        account = trading_client.get_account()
        dir(account)
        account.__pretty__
        float(account.buying_power)
        float(account.equity)
        float(account.last_equity) # balance at the last market close
        # Check our current balance vs. our balance at the last market close
        balance_change = round(float(account.equity) - float(account.last_equity),2) 

        
        float(account.portfolio_value)
        account.buying_power # buying power is 2x equity if 2000 < equity < 25000, and 4x equity if equity > 25000
        account.regt_buying_power # regulation T buying power = (2 x equity) - (long_position_value - short_position_value)
        account.daytrading_buying_power
        is_day_trade = True
        buying_power = float(account.daytrading_buying_power) if is_day_trade else float(account.regt_buying_power)

        account.cash # account.cash value should generally not be used for much of anything. 
                     # it becomes meaningless if one ever shorts a position

        account.daytrade_count
        account.initial_margin
        
        account.short_market_value
        account.shorting_enabled
        account.sma


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



        # Get fees
            # the fees post the day after the trading
            # Since I'm trying to look at a daily P&L net of fees, ...
            # ... I need to adjust those fee dates back one trading day, ...
            # ... which gets a touch complicated with weekends and holidays


# Submit orders
        # Understanding orders: https://alpaca.markets/docs/trading-on-alpaca/orders/
        # Nice overview of different order types: https://alpaca.markets/learn/13-order-types-you-should-know-about/
        # From Feb 2022 Alpaca allows trading from 4am till 8pm ET (10am - 2am Munich Time)
                    # Rules for submitting orders for extended hours: https://alpaca.markets/docs/trading/orders/#extended-hours-trading
                    # extended hours order must be DAY limit orders: extended_hours=True & type='limit' & time_in_force=TimeInForce.DAY
                            # A limit orders with a limit price that significantly exceeds the current market price will be rejected
                            # Any other order types, including market orders, will be rejected
                    # Your extended hour order will be processed and filled immediately.
            # Orders not eligible for extended hours 
            #       submitted between 4:00pm - 7:00pm ET will be rejected.
            #       submitted after 7:00pm ET will be queued and eligible for execution at the time of the next market open

        dir(TimeInForce)
        # Time in force: https://alpaca.markets/docs/trading/orders/#time-in-force
            '''
            gtc - good-till-cancelled
            day - eligible for execution only on the day it is live (9:30am - 4:00pm ET)
            opg - use it to submit “market on open” (MOO) and “limit on open” (LOO) orders: must be submitted after 7:00pm and before 9:28am
            cls - use it to submit “market on close” (MOC) and “limit on close” (LOC) orders
            ioc - Immediate Or Cancel (IOC) - all or part of the order to be executed immediately
            fok - Fill or Kill (FOK)
            '''

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


# Order list, executed trades, positions

        dir(OrderStatus) # 'ACCEPTED', 'ACCEPTED_FOR_BIDDING', 'CALCULATED', 'CANCELED', 
                        # 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED', 'REPLACED', 'FILLED', 'NEW', 'PARTIALLY_FILLED', 
                        # 'PENDING_CANCEL', 'PENDING_NEW', 'PENDING_REPLACE',  'STOPPED', 'SUSPENDED'
            # full list of statuses: https://alpaca.markets/docs/trading-on-alpaca/orders/#order-lifecycle
                # Updates on orders states at Alpaca will be sent over the streaming interface

        date_filter = (pd.Timestamp.now()- pd.Timedelta(90, "days")).floor(freq='S') # orders for last 30 days
        date_filter = (pd.Timestamp.now()- pd.Timedelta(7, "hours")).floor(freq='S') # orders for last 30 days
        date_filter = (pd.Timestamp.now()- pd.Timedelta(5, "minutes")).floor(freq='S'), # orders for last 5 minutes
        date_filter = (pd.Timestamp.now()- pd.Timedelta(30, "days")).floor(freq='S').isoformat() # isoformat also works


        request_params = GetOrdersRequest(status=QueryOrderStatus.CLOSED, # Not the same as OrderStatus
                                        # status=QueryOrderStatus.OPEN, # Not the same as OrderStatus
                                        after = date_filter,
                                        # side=OrderSide.BUY, # optional
                                        # symbols = ['SPY','QQQ'], # optional
                                        )
        orders = trading_client.get_orders(filter=request_params)


        orders_dicts = []
        for entry in orders:
            print(entry.client_order_id)
            entry_dict = {}  # Create a new dictionary for each entry
            for attribute, value in vars(entry).items():
                entry_dict[attribute] = value  # Populate the dictionary
            orders_dicts.append(entry_dict)  # Append the populated dictionary to the list
        df = pd.DataFrame(orders_dicts)
        for column in df.columns:
            if pd.api.types.is_datetime64tz_dtype(df[column]):
                df[column] = df[column].dt.tz_localize(None)
        df.to_excel('orders3.xlsx')
        df.columns

        df[['filled_at','symbol','filled_qty','order_type','side']]

        df['side'] = 



        ticker = 'JAKK'
        request_params = GetOrdersRequest(status=QueryOrderStatus.OPEN, side=OrderSide.SELL, symbols = [ticker,'PGTI'])
        orders = trading_client.get_orders(filter=request_params)
        positions_dict_tp = sum([int(position.qty) for position in orders])









        columns_to_convert = ['created_at','updated_at','submitted_at','filled_at','expired_at','canceled_at','failed_at','replaced_at']
        for col in columns_to_convert:
            orders[col] = orders[col].str[:24] # microsecond level
            orders[col] = pd.to_datetime(orders[col], errors='coerce')
            mask = orders[col].notna()
            orders.loc[mask, col] = orders.loc[mask, col].dt.tz_localize('US/Eastern').dt.tz_convert('Europe/Berlin').dt.tz_localize(None)

        columns_to_convert_float = ['trail_price', 'hwm', 'trail_percent', 'stop_price', 'limit_price', 'filled_avg_price']
        orders[columns_to_convert_float] = round(orders[columns_to_convert_float].astype(float),2)
        columns_to_convert_integer = ['qty', 'filled_qty']
        orders[columns_to_convert_integer] = orders[columns_to_convert_integer].astype(int)
        df.to_csv('orders.csv')


    # filter by client id

        trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER) # dir(trading_client)
        # to more easily convert the portfolio to a dataframe, instantiate the TradingClient with raw_data=True
        trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER, paper=True, raw_data=True)
        orders = trading_client.get_orders()
        search_string = 'esting'
        # below iteration doesn't work if trading_client has "raw_data = True"
        filtered_orders = [order for order in orders if search_string in order.client_order_id]

    SPY_orders = [o for o in orders if o.symbol == 'SPY']
    open_sell_symbols = {order.symbol for order in orders if order.side == "buy" and order.status == "canceled" and order.filled_at == None}


    # cancel all not trailing orders: 
        orders = api.list_orders(status='open')
        for order in orders:
            if order.symbol in symbol and order.type != 'trailing_stop':
                api.cancel_order(order.id)
                print(f"Cancelling order {order.symbol} with ID {order.id}")



    # current positions
        positions = trading_client.get_all_positions()
            # Returned json per position:
                    {
                    "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL ",
                    "exchange": "NASDAQ",
                    "asset_class": "us_equity",
                    "avg_entry_price": "100.0", # details: https://docs.alpaca.markets/docs/position-average-entry-price-calculation#which-method-is-alpaca-using
                    "qty": "5",
                    "qty_available": "4",
                    "side": "long",
                    "market_value": "600.0",
                    "cost_basis": "500.0", # details: https://docs.alpaca.markets/docs/position-average-entry-price-calculation#which-method-is-alpaca-using
                    "unrealized_pl": "100.0",
                    "unrealized_plpc": "0.20",
                    "unrealized_intraday_pl": "10.0",
                    "unrealized_intraday_plpc": "0.0084",
                    "current_price": "120.0",
                    "lastday_price": "119.0",
                    "change_today": "0.0084"
                    }



        positions =[]
        if positions:
            print("hi")
        positions_symbols_set = {p.symbol for p in positions}
        [print(f"{p.symbol} with profit of {p.unrealized_pl}",end=";\n") for p in positions]


        position_dicts = []
        for entry in positions:
            entry_dict = {}  # Create a new dictionary for each entry
            for attribute, value in vars(entry).items():
                entry_dict[attribute] = value  # Populate the dictionary
            position_dicts.append(entry_dict)  # Append the populated dictionary to the list
        df = pd.DataFrame(position_dicts)
        cols_to_number = ['avg_entry_price','qty','market_value','cost_basis','unrealized_pl',
                          'unrealized_plpc','unrealized_intraday_pl','unrealized_intraday_plpc',
                          'current_price','lastday_price','change_today','qty_available']
        for column in cols_to_number:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        cols_needed = ['symbol','avg_entry_price','qty','side','market_value','cost_basis','unrealized_pl',
                          'unrealized_plpc','unrealized_intraday_pl','unrealized_intraday_plpc',
                          'current_price','lastday_price','change_today','qty_available']
        df = df[cols_needed]
        df.to_excel('positions.xlsx')




        stop_loss = -0.05
        positions_symbols_to_close = [p.symbol for p in positions if float(p.unrealized_plpc)<stop_loss]


        try:
            position = trading_client.get_open_position('XLF').qty
        except: # No position exists
            position = 0




        # how often is the trading_client.get_all_positions() updated?

            # Check how long for position to show up after a fill
            # First get the current position qty
            symbol = 'SPY'
            positions_list = trading_client.get_all_positions()
            positions_df = pd.DataFrame([position for position in positions_list])

            if positions_df.query('symbol==@symbol').empty:
                initial_qty = 0
            else:
                initial_qty = positions_df.query('symbol==@symbol').qty.astype('int').iat[0]

            # Next place an order
            market_order_data = MarketOrderRequest(symbol=symbol,qty=1,side=OrderSide.BUY,time_in_force=TimeInForce.DAY)
            my_order = trading_client.submit_order(market_order_data)

            # Poll to see when order fills
            status = None
            while status != 'filled':
                my_order = trading_client.get_order_by_id(my_order.get('id'))
                status = my_order.get('status')

            # order filled
            created_at = pd.to_datetime(my_order.get('created_at')).tz_convert(None)
            filled_at = pd.to_datetime(my_order.get('filled_at')).tz_convert(None)
            current_time = pd.to_datetime('now', utc=True).tz_convert(None)

            delta_fill_time = (current_time - created_at).total_seconds()
            delta_poll_time = (current_time - filled_at).total_seconds()

            display('{} seconds to fill'.format(delta_fill_time))
            display('{} seconds polled after fill and status is {} '.format(delta_poll_time, status))

            # Once filled check to see if it's in the portfolio
            # Poll until order shows up as filled in the list
            filled_in_portfolio = False

            while not filled_in_portfolio:
                # Get the portfolio and check if qty incremented
                positions_list = trading_client.get_all_positions()
                positions_df = pd.DataFrame([position for position in positions_list])

            if positions_df.query('symbol==@symbol').empty:
                new_qty = 0
            else:
                new_qty = positions_df.query('symbol==@symbol').qty.astype('int').iat[0]

            if new_qty > initial_qty:
                filled_in_portfolio = True
                status = 'in portfolio'
            else:
                filled_in_portfolio = False
                status = 'not in portfolio yet'    

            current_time = pd.to_datetime('now', utc=True).tz_convert(None)
            delta_time = (current_time - filled_at).total_seconds()

            display('{} seconds after fill and status is {} '.format(delta_time, status))

            display('filled')


    # Account activities

        endpoint = '/account/activities'
        pd.DataFrame(trading_client.get(path=endpoint)).to_excel('activities.xlsx')
        activity = 'FEE'
        endpoint = f'/account/activities/{activity}'
        params = {'after':'2023-11-30','until':'2023-12-20'}
        pd.DataFrame(trading_client.get(path=endpoint, data=params))

        # Activity types:
            'FILL' # Order fills (both partial and full fills)
            'TRANS' # Cash transactions (both CSD and CSW)
            'MISC' # Miscellaneous or rarely used activity types (All types except those in TRANS, DIV, or FILL)
            'ACATC' # ACATS IN/OUT (Cash)
            'ACATS' # ACATS IN/OUT (Securities)
            'CFEE' # Crypto fee
            'CSD' # Cash deposit(+)
            'CSW' # Cash withdrawal(-)
            'DIV' # Dividends
            'DIVCGL' # Dividend (capital gain long term)
            'DIVCGS' # Dividend (capital gain short term)
            'DIVFEE' # Dividend fee
            'DIVFT'     # Dividend adjusted (Foreign Tax Withheld)
            'DIVNRA'    # Dividend adjusted (NRA Withheld)
            'DIVROC'    # Dividend return of capital
            'DIVTW'    # Dividend adjusted (Tefra Withheld)
            'DIVTXEX'    # Dividend (tax exempt)
            'FEE'    # Fee denominated in USD
            'INT'    # Interest (credit/margin)
            'INTNRA'    # Interest adjusted (NRA Withheld)
            'INTTW'    # Interest adjusted (Tefra Withheld)
            'JNL'    # Journal entry
            'JNLC'    # Journal entry (cash)
            'JNLS'    # Journal entry (stock)
            'MA'    # Merger/Acquisition
            'NC'    # Name change
            'OPASN'    # Option assignment
            'OPEXP'    # Option expiration
            'OPXRC'    # Option exercise
            'PTC'    # Pass Thru Charge
            'PTR'    # Pass Thru Rebate
            'REORG'    # Reorg CA
            'SC'    # Symbol change
            'SSO'    # Stock spinoff
            'SSP'    # Stock split



# Assets
    # 'shortable' = True, 'easy_to_borrow' = True, 'marginable' = True, asset_class=None
    # Alpaca currently uses its clearing firms ‘Easy To Borrow’ list and assumes everything else is Hard To Borrow. 
    # However there is no historical data for which stocks were hard to borrow earlier.

    assets = trading_client.get_all_assets(
                                GetAssetsRequest(
                                    asset_class=AssetClass.US_EQUITY, # AssetClass.CRYPTO
                                    status= AssetStatus.ACTIVE
                                    )
                                )
 
    assets[1]
    assets_mr = [asset.maintenance_margin_requirement for asset in assets]
    from collections import Counter
    Counter(assets_mr)


    exclude_strings = ['Etf', 'ETF', 'Lp', 'L.P', 'Fund', 'Trust', 'Depositary', 'Depository', 'Note', 'Reit', 'REIT']
    assets_in_scope = [asset for asset in assets
                        if asset.exchange != 'OTC' # OTC stocks play by different rules than Exchange Traded stocks (often referred to as NMS). 
                                                   # Data reporting is even different. 
                                                   # It’s not that one shouldn’t trade OTC stocks but be aware they have different rules. 
                                                   # Perhaps separate OTC stocks in your algo if you wish to trade these. 
                        and asset.shortable
                        and asset.tradable
                        and asset.marginable # if a stock is not marginable that means it cannot be used as collateral for margin. 
                                                # As an example, if one had equity of $10,000 one could buy $10,000 of marginable stock and still have $10,000 left in buying power. 
                                                # However, if one were to buy $10,000 of non marginable stock the buying power would be $0 (ie no funds can be margined against that stock). 
                                                # This has ramifications for RegT buying power and Fed margin calls. Best to stay away from these unless your algo carefully monitors RegT buying power. 
                                                # FYI there are currently no tradable exchange traded stocks which are not marginable (so this doesn’t really limit the universe). However, all OTC stocks are non marginable (another reason to perhaps not consider OTC stocks)
                        and asset.fractionable # indirectly filters out a lot of small volatile stocks:  
                                                # Alpaca Trading Team manually reviews stocks to qualify as ‘fractionable’. 
                                                # This is entirely an Alpaca designation and other brokers may have different ‘fractionable’ stocks
                        and asset.easy_to_borrow 
                        and asset.maintenance_margin_requirement == 30
                        and not (any(ex_string in asset.name for ex_string in exclude_strings))]
    len(assets), len(assets_in_scope)


    positions = trading_client.get_all_positions()
    assets_in_pf = pd.concat((pd.DataFrame(trading_client.get_asset(position.symbol)).set_index(0) for position in positions),axis=1)

    ticker = 'AMD'
    trading_client.get_asset(ticker).easy_to_borrow*trading_client.get_asset(ticker).shortable


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


# Corporate announcements

    trading_client.get_corporate_announcements(
        GetCorporateAnnouncementsRequest(
            ca_types=['merger'],
            since = '2023-09-11', # 90 days
            until = '2023-10-11', # 90 days
            date_type = 'declaration_date', # declaration_date, ex_date, record_date, payable_date
        )) # error

    dir(CorporateActionType)
    '''
    Details: https://alpaca.markets/docs/api-references/trading-api/corporate-actions-announcements/
    '''

    trading_client.get_corporate_announcement_by_id() # error


# Market data stocks ----------------------------------------------------------------------------------------------------------------

        # Snapshot
            scope_tickers = Universes.TOP10_US_SECTOR
            snap = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=scope_tickers, feed = DataFeed.SIP))
            snapshot_data = {stock: [snapshot.latest_trade.price, 
                                    snapshot.previous_daily_bar.close,
                                    snapshot.daily_bar.close,
                                    (snapshot.daily_bar.close/snapshot.previous_daily_bar.close)-1,
                                    ]
                            for stock, snapshot in snap.items() if snapshot and snapshot.daily_bar and snapshot.previous_daily_bar
                            }
            snapshot_columns=['price', 'prev_close', 'last_close', 'gain']
            snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)
            top_gainers_over_3_dollars = snapshot_df.query('price>3').nlargest(10, 'gain')


            SPY = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=['SPY'], feed = DataFeed.SIP))
            now_time = SPY['SPY'].daily_bar.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            yesterday_close_time = SPY['SPY'].previous_daily_bar.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            symbols = ['SPY','NVDA','F']
            snap = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=symbols, feed = DataFeed.SIP))
            snapshot_data = {stock: [
                                    snapshot.latest_trade.timestamp,                        
                                    snapshot.latest_trade.price, 
                                    snapshot.daily_bar.timestamp,
                                    snapshot.daily_bar.open,
                                    snapshot.daily_bar.close,
                                    snapshot.previous_daily_bar.timestamp,
                                    snapshot.previous_daily_bar.close,
                                    ]
                            for stock, snapshot in snap.items() if snapshot and snapshot.daily_bar and snapshot.previous_daily_bar
                            }
            snapshot_columns=['price time', 'price', 'today', 'today_open', 'today_close','yest', 'yest_close']
            snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)

            snapshot_df['price time'] = snapshot_df['price time'].dt.tz_convert('America/New_York').dt.tz_localize(None) # convert from UTC to ET and remove +00:00 from datetime
            snapshot_df['price time short'] = snapshot_df['price time'].dt.strftime('%H:%M')
            snapshot_df['today'] = snapshot_df['today'].dt.tz_convert('America/New_York').dt.tz_localize(None)
            snapshot_df['yest'] = snapshot_df['yest'].dt.tz_convert('America/New_York').dt.tz_localize(None)
            snapshot_df = snapshot_df.reset_index().rename(columns={'index':'symbol'}) # needed for merger on symbol
            snapshot_df['price time short'][0]

            max_time_str = snapshot_df['price time short'].max()
            max_time = dt.datetime.strptime(max_time_str, '%H:%M').time()
            target_time = dt.time(9, 30)

            # Calculate the difference in hours
            if max_time > target_time:
                diff = (dt.datetime.combine(dt.datetime.today(), dt.time(23, 59, 59, 999999)) - dt.datetime.combine(dt.datetime.today(), max_time)) + dt.timedelta(hours=9, minutes=30)
            else:
                diff = dt.datetime.combine(dt.datetime.today(), target_time) - dt.datetime.combine(dt.datetime.today(), max_time)

            hours_left_till_open = diff.total_seconds() / 3600  # Convert timedelta to hours


            tickers = ['SPY','NVDA','F']
            clock = trading_client.get_clock()
            today = clock.timestamp
            snapshots_dict = {}
            CHUNK_SIZE = 1000 # There is a maximum length a URI can be => so get the snapshots in 'chunks'
            for chunk_start in range(0, len(assets_in_scope), CHUNK_SIZE):
                chunk_end = chunk_start + CHUNK_SIZE
                chunk = assets_in_scope[chunk_start:chunk_end]
                snapshots_chunk = self.stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=chunk, feed = DataFeed.SIP))
                snapshots_dict.update(snapshots_chunk)

            snapshot_data = {stock: [
                                    snapshot.latest_trade.timestamp,                        
                                    snapshot.latest_trade.price, 
                                    snapshot.daily_bar.open,
                                    snapshot.daily_bar.close,
                                    snapshot.previous_daily_bar.close,
                                    ]
                            for stock, snapshot in snapshots_dict.items() if snapshot and snapshot.daily_bar and snapshot.previous_daily_bar
                            }
            snapshot_columns=['price time', 'price', 'today_open', 'today_close','yest_close']
            snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)
            snapshot_df['price time'] = snapshot_df['price time'].dt.tz_convert('America/New_York').dt.tz_localize(None) # convert from UTC to ET and remove +00:00 from datetime
            snapshot_df = snapshot_df.reset_index().rename(columns={'index':'symbol'}) # needed for merger on symbol

            if clock.is_open: # open market
                snapshot_df['PRE'] = round(100*(snapshot_df['today_open']-snapshot_df['yest_close'])/snapshot_df['yest_close'],1) # from close (22:00 MUC) on previous trade day till today open (15:30 MUC)
                snapshot_df['DAY'] = round(100*(snapshot_df['price']-snapshot_df['today_open'])/snapshot_df['today_open'],1) # current vs open
                snapshot_df['POST'] = 0 # no data yet as the market is stil open
            elif snapshot_df['price time'].max().date() == today.date():
                if today.hour >= 16: # post market
                    snapshot_df['PRE'] = round(100*(snapshot_df['today_open']-snapshot_df['yest_close'])/snapshot_df['yest_close'],1) # # from close (22:00 MUC) on previous trade day till today open (15:30 MUC)
                    snapshot_df['DAY'] = round(100*(snapshot_df['today_close']-snapshot_df['today_open'])/snapshot_df['today_open'],1) # from 15:30 till 22:00 Munich time
                    snapshot_df['POST'] = round(100*(snapshot_df['price']-snapshot_df['today_close'])/snapshot_df['today_close'],1) # today from 22:00 MUC (16:00 ET) till now
                else: # pre-market
                    snapshot_df['PRE'] = round(100*(snapshot_df['price']-snapshot_df['today_close'])/snapshot_df['today_close'],1) # this include also yesterday'S post, but what could I do...
                    snapshot_df['DAY'] = 0 # no data yet: some time till open in US
                    snapshot_df['POST'] = 0 # no data yet: Yesterday POST is included in current PRE
            else: # weekends, holidays
                snapshot_df['PRE'] = 0
                snapshot_df['DAY'] = 0
                snapshot_df['POST'] = round(100*(snapshot_df['price']-snapshot_df['today_close'])/snapshot_df['today_close'],1) # POST return for latest available trading date

            snapshot_df.reset_index(drop=True, inplace=True)








        # Bars

            # Main

                today = trading_client.get_clock().timestamp
                previous_day = today - pd.Timedelta('1D')
                previous_day_40 = today - pd.Timedelta('40D')

                clock = trading_client.get_clock()
                end_day = clock.timestamp
                start_day = clock.timestamp.date() - relativedelta(days=50)

                start = dt.datetime.strptime("2023-05-20", '%Y-%m-%d')


                bars_request_params = StockBarsRequest(symbol_or_symbols=['AAPL','F','NVDA'], 
                                                       start = start_day, end = end_day, 
                                                        # limit = 100, # upper limit of number of data points
                                                       timeframe=TimeFrame.Hour, # 'Day', 'Hour', 'Minute', 'Month', 'Week'
                                                       adjustment= Adjustment.ALL, # SPLIT, DIVIDEND, ALL
                                                       feed = DataFeed.SIP)
                data_df = stock_client.get_stock_bars(bars_request_params).df.reset_index()
                data_df.timestamp = data_df.timestamp.dt.tz_convert('America/New_York').dt.tz_localize(None) # Convert to market time and remove +00:00
                data_df = data_df[data_df['timestamp'].dt.time.between(pd.to_datetime('09:30:00').time(), pd.to_datetime('16:00:00').time())]


            # get 5 latest minute bars
                bars_request_params = StockBarsRequest(
                    symbol_or_symbols=['SPY'], 
                    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(2, "days")), end = pd.Timestamp.now(),
                    timeframe=TimeFrame.Minute, # 'Day', 'Hour', 'Minute', 'Month', 'Week'
                    adjustment= Adjustment.RAW, # SPLIT, DIVIDEND, ALL
                    feed = DataFeed.SIP,
                    limit = 5, # upper limit of number of data points
                    sort=Sort.DESC
                    )
                hist_bars = stock_client.get_stock_bars(bars_request_params).df.reset_index()
                hist_bars.timestamp = hist_bars.timestamp.dt.tz_convert('America/New_York').dt.tz_localize(None) 
                                                                # Convert to market time for easier reading
                                                                                            # remove +00:00 from datetime


            # for not-standard timeframes (e.g. 5min, 30min)
                minute_frame = 30
                bars_request_params = StockBarsRequest(
                    symbol_or_symbols=Universes.Spiders, 
                    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(6, "days")).floor(freq='S'), # T - for minutes, H - for hours
                    end = pd.Timestamp.now(), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
                    # limit = 100, # upper limit of number of data points
                    timeframe=TimeFrame(minute_frame, TimeFrameUnit.Minute), # 'Day', 'Hour', 'Minute', 'Month', 'Week'
                    adjustment= Adjustment.RAW, # SPLIT, DIVIDEND, ALL
                    feed = DataFeed.SIP
                    )
                hist_bars = stock_client.get_stock_bars(bars_request_params).df.reset_index()
                hist_bars.timestamp = hist_bars.timestamp.dt.tz_convert('America/New_York').dt.tz_localize(None) 
                                                                # Convert to market time for easier reading
                                                                                            # remove +00:00 from datetime

                # get weekly data
                    today = trading_client.get_clock().timestamp
                    start = dt.datetime.strptime(row["Date"], '%Y-%m-%d')
                    bars_request_params = StockBarsRequest(symbol_or_symbols=symbol_list+['SPY'], 
                                                        start=start, end=today, 
                                                        timeframe=TimeFrame(1, TimeFrameUnit.Week), # 'Day', 'Hour', 'Minute', 'Month', 'Week'
                                                        adjustment= Adjustment.ALL,
                                                        feed=DataFeed.SIP)
                    weekly_df = stock_client.get_stock_bars(bars_request_params).df
                    weekly_df = weekly_df.reset_index()
                    weekly_df.timestamp = weekly_df.timestamp.dt.date
                    weekly_df['Weekly Return'] = weekly_df.groupby('symbol')['close'].pct_change()
                    weekly_df.dropna(inplace=True)
                    weekly_fig = px.bar(weekly_df, x='timestamp', y='Weekly Return', color='symbol', barmode='group')




                bars_request_params = StockBarsRequest(
                    symbol_or_symbols=Universes.Spiders, 
                    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(600, "days")).floor(freq='S'), # T - for minutes, H - for hours
                    end = pd.Timestamp.now(), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
                    # limit = 100, # upper limit of number of data points
                    timeframe=TimeFrame(1, TimeFrameUnit.Week), # 'Day', 'Hour', 'Minute', 'Month', 'Week'
                    adjustment= Adjustment.RAW, # SPLIT, DIVIDEND, ALL
                    feed = DataFeed.SIP
                    )
                hist_bars = stock_client.get_stock_bars(bars_request_params).df.reset_index()


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


            # Latest bars
                latest_bars = stock_client.get_stock_latest_bar(StockLatestBarRequest(symbol_or_symbols=["SPY", "GLD", "TLT"], feed = DataFeed.SIP))
                latest_bars['SPY']['c']
                pd.DataFrame(latest_bars).T


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
