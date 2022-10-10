# New SDK: https://github.com/alpacahq/alpaca-py

import Universes
from Alpaca_config import *

import pandas as pd
import time
import datetime as dt
import random

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderStatus
from alpaca.trading.requests import GetAssetsRequest, GetOrdersRequest, MarketOrderRequest, LimitOrderRequest, StopLossRequest, TrailingStopOrderRequest
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, StockTradesRequest, StockQuotesRequest, StockBarsRequest, StockSnapshotRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import Adjustment, DataFeed, Exchange
import alpaca
alpaca.__version__

trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER) # dir(trading_client)
stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)



# Account information

        account = trading_client.get_account()
        dir(account)
        float(account.equity)
        float(account.last_equity) # balance at the last market close
        # Check our current balance vs. our balance at the last market close
        balance_change = round(float(account.equity) - float(account.last_equity),2) 

        account.portfolio_value
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


        # Get fees
            # the fees post the day after the trading
            # Since I'm trying to look at a daily P&L net of fees, ...
            # ... I need to adjust those fee dates back one trading day, ...
            # ... which gets a touch complicated with weekends and holidays


# Orders
        # https://alpaca.markets/docs/trading-on-alpaca/orders/

        # Submit orders
                # Nice overview of different order types: https://alpaca.markets/learn/13-order-types-you-should-know-about/
                # From Feb 2022 Alpaca allows trading from 4am till 8pm ET (10am - 2am Munich Time)
                            # Rules for submitting orders for extended hours: https://alpaca.markets/docs/trading-on-alpaca/orders/#extended-hours-trading
                            # extended_hours=True & type='limit' & time_in_force=TimeInForce.DAY:
                                    # A limit orders with a limit price that significantly exceeds the current market price will be rejected
                                    # Any other order types, including market orders, will be rejected
                                    # extended hours order must be DAY limit orders
                            # Your extended hour order will be processed and filled immediately.
                    # Orders not eligible for extended hours 
                    #       submitted between 4:00pm - 7:00pm ET will be rejected.
                    #       submitted after 7:00pm ET will be queued and eligible for execution at the time of the next market open

                dir(TimeInForce)
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
                ticker = 'SPY'
                ticker = random.choice(Universes.Spiders)
                limit_buy = 0.99 # how limit price should be different from current ask
                stop_loss = trail_sl = 5 # in %, easy stop-loss, I should use ATR() like in zorro probably
                
                latest_quote = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[ticker]))
                limit_price_target = round(latest_quote[ticker].ask_price*limit_buy,0)
                quantity = int(float(account.buying_power)//latest_quote[ticker].ask_price)


                # Market order
                    market_order_data = MarketOrderRequest(
                        symbol=ticker,
                        qty=quantity,
                        side=OrderSide.BUY, # dir(OrderSide)
                        # extended_hours = True, # not possible for market order
                        client_order_id = coid,
                        stop_loss = StopLossRequest(stop_price=limit_price_target*(1-stop_loss/100)),
                        time_in_force=TimeInForce.GTC) # dir(TimeInForce)
                    market_order = trading_client.submit_order(order_data=market_order_data)
                    print(f"Buying {quantity} of {ticker}. Buing power left is {float(account.buying_power)}")

                # Trailing order
                    trailing_sl_order_data = TrailingStopOrderRequest(
                        symbol=ticker,
                        qty=3,
                        side=OrderSide.BUY, # dir(OrderSide)
                        # extended_hours = True, # not possible for traling order
                        client_order_id = 'testing trailing order 1',
                        trail_percent=trail_sl,
                        time_in_force=TimeInForce.GTC) # dir(TimeInForce)
                    trailing_order = trading_client.submit_order(order_data=trailing_sl_order_data)

                # Limit order
                    limit_order_data = LimitOrderRequest(symbol=ticker,
                                                        limit_price=limit_price_target,
                                                        qty=1,
                                                        side=OrderSide.BUY,
                                                        extended_hours = True,
                                                        client_order_id = coid,
                                                        stop_loss = StopLossRequest(stop_price=limit_price_target*(1-stop_loss)),
                                                        time_in_force=TimeInForce.DAY) # dir(TimeInForce), extended hours order must be DAY limit orders
                    limit_order = trading_client.submit_order(order_data=limit_order_data)


                trading_client.close_position('SPY') 
                trading_client.close_all_positions(cancel_orders=True) # closes all position AND also cancels all open orders


                trading_client.cancel_orders() 
                trading_client.cancel_order_by_id() 
                trading_client.replace_order_by_id()



            '''
            How to switch from long to short? 
            2 orders are required: 
                (Option A) Send 1st order. Listen on web socket for order ack. Then send 2nd order. 
                (Option B) Send 1st order. Sleep for some configurable amount of time. Then send 2nd order. Retry 2nd order if rejected for ‘invalid qty’. 
            '''


        # Order list, executed trades, positions

                dir(OrderStatus) # 'ACCEPTED', 'ACCEPTED_FOR_BIDDING', 'CALCULATED', 'CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED', 'REPLACED',
                                # 'FILLED', 'NEW', 'PARTIALLY_FILLED', 'PENDING_CANCEL', 'PENDING_NEW', 'PENDING_REPLACE',  'STOPPED', 'SUSPENDED'
                    # full list of statuses: https://alpaca.markets/docs/trading-on-alpaca/orders/#order-lifecycle
                        # Updates on orders states at Alpaca will be sent over the streaming interface


                date_filter = (pd.Timestamp.now()- pd.Timedelta(30, "days")).floor(freq='S') # orders for last 30 days
                date_filter = (pd.Timestamp.now()- pd.Timedelta(5, "minutes")).floor(freq='S'), # orders for last 5 minutes
                date_filter = (pd.Timestamp.now()- pd.Timedelta(30, "days")).floor(freq='S').isoformat() # isoformat also works

            # extract all orders to df
                request_params = GetOrdersRequest(status='all', # only 'open', 'closed', 'all'. Not the same as OrderStatus
                                                after = date_filter,
                                                side=OrderSide.BUY)
                orders = trading_client.get_orders(filter=request_params)
                orders_df = pd.concat((pd.DataFrame(order).set_index(0) for order in orders),axis=1).T
                columns_to_convert = ['created_at','updated_at','submitted_at','filled_at','expired_at','canceled_at','failed_at','replaced_at']
                for column in columns_to_convert:
                    try:
                        orders_df[column] = orders_df[column].dt.ceil(freq='s').dt.tz_convert('Europe/Berlin').dt.tz_localize(None)
                                                            # round till seconds
                                                                                # convert from UTC to local time 
                                                                                                                # remove time zone
                    except: # this we need as empty columns could give error
                        pass
                orders_df.to_excel('orders.xlsx')

            # filter by client id
                search_string = 'esting'
                filtered_orders_df = pd.concat((pd.DataFrame(order).set_index(0) for order in orders if search_string in order.client_order_id),axis=1).T

                # So these 2 below are not really needed. Or maybe to check the Order Status of a recently sent order, which ID is still in variable
                    trading_client.get_order_by_client_id()
                    trading_client.get_order_by_id()

            SPY_orders = [o for o in orders if o.symbol == 'SPY']
            open_sell_symbols = {order.symbol for order in orders if order.side == "buy" and order.status == "canceled" and order.filled_at == None}


            # current positions
                positions = trading_client.get_all_positions()
                positions_df = pd.concat((pd.DataFrame(position).set_index(0) for position in positions),axis=1)
                positions_df = positions_df.T.apply(pd.to_numeric, errors='ignore').T # convert strings to numeric
                positions_df[['unrealized_plpc','unrealized_intraday_plpc','change_today']] = positions_df[['unrealized_plpc','unrealized_intraday_plpc','change_today']].applymap("{0:.2f}".format)

                trading_client.get_open_position('XLF')


# Assets
    # 'shortable' = True, 'easy_to_borrow' = True, 'marginable' = True, asset_class=None
    # Alpaca currently uses its clearing firms ‘Easy To Borrow’ list and assumes everything else is Hard To Borrow. 
    # However there is no historical data for which stocks were hard to borrow earlier.

    assets = trading_client.get_all_assets(
                                GetAssetsRequest(
                                    asset_class=AssetClass.US_EQUITY,
                                    status= AssetStatus.ACTIVE
                                    )
                                )
 
    exclude_strings = ['Etf', 'ETF', 'Lp', 'L.P', 'Fund', 'Trust', 'Depositary', 'Depository', 'Note', 'Reit', 'REIT']
    assets_in_scope = [asset.symbol for asset in assets
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
                        and not (any(ex_string in asset.name for ex_string in exclude_strings))]
    len(assets), len(assets_in_scope)
    assets[500].maintenance_margin_requirement


    positions = trading_client.get_all_positions()
    assets_in_pf = pd.concat((pd.DataFrame(trading_client.get_asset(position.symbol)).set_index(0) for position in positions),axis=1)

    ticker = 'AMD'
    trading_client.get_asset(ticker).easy_to_borrow*trading_client.get_asset(ticker).shortable




# ------------------------------------------------------------


trading_client.get_calendar()
trading_client.get_clock() # errror


trading_client.get_watchlists()
trading_client.get_watchlist_by_id()
trading_client.create_watchlist()
trading_client.delete_watchlist_by_id()
trading_client.update_watchlist_by_id()
trading_client.add_asset_to_watchlist_by_id()
trading_client.remove_asset_from_watchlist_by_id()




trading_client.get_corporate_annoucements() # error
trading_client.get_corporate_announcment_by_id() # error








# Streaming Trade Updates
    from alpaca.trading.stream import TradingStream
    trading_stream = TradingStream(API_KEY_PAPER,API_SECRET_PAPER, paper=True)
    async def update_handler(data):     # trade updates will arrive in our async handle
        print(data)
    trading_stream.subscribe_trade_updates(update_handler)
    trading_stream.run()



# Market data stocks ----------------------------------------------------------------------------------------------------------------

stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)

bars_request_params = StockBarsRequest(
    symbol_or_symbols=["SPY"], 
    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(2, "days")).floor(freq='S'), # T - for minutes, H - for hours
    end = pd.Timestamp.now(), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
    # limit = 100, # upper limit of number of data points
    timeframe=TimeFrame.Minute, # 'Day', 'Hour', 'Minute', 'Month', 'Week'
    adjustment= Adjustment.RAW, # SPLIT, DIVIDEND, ALL
    feed = DataFeed.SIP
    )
hist_bars = stock_client.get_stock_bars(bars_request_params).df.droplevel(level=0) # drop level is needed as 1st it appears with multiindex with symbol
hist_bars.index = hist_bars.index.tz_convert('America/New_York') # Convert to market time for easier reading
hist_bars.index = hist_bars.index.tz_localize(None) # remove +00:00 from datetime



latest_bars = stock_client.get_stock_latest_bar(StockLatestBarRequest(symbol_or_symbols=["SPY", "GLD", "TLT"], feed = DataFeed.SIP))
latest_bars['SPY'].open # ask_exchange, ask_price, ask_size, bid_exchange, bid_price, bid_size, conditions, tape, timestamp
pd.DataFrame(latest_bars) # How to put this to DF?
pd.DataFrame(latest_bars['SPY'])


latest_multisymbol_quotes = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=["SPY", "GLD", "TLT"]))
latest_multisymbol_quotes["GLD"].ask_price # ask_exchange, ask_price, ask_size, bid_exchange, bid_price, bid_size, conditions, tape, timestamp
pd.json_normalize(latest_multisymbol_quotes)


snapshot_stocks = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=["SPY", "GLD", "TLT"], feed = DataFeed.SIP))
snapshot_stocks['SPY']

trades_request_params = StockTradesRequest(
    symbol_or_symbols=["SPY"], 
    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(60, "minutes")).floor(freq='S'), # T - for minutes, H - for hours
    end = pd.Timestamp.now(tz="US/Eastern"), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
    limit = 100, # upper limit of number of data points
    feed = DataFeed.SIP
    )
latest_multisymbol_quotes = stock_client.get_stock_trades(trades_request_params).df


quotes_request_params = StockQuotesRequest(
    symbol_or_symbols=["SPY"], 
    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(60, "minutes")).floor(freq='S'), # T - for minutes, H - for hours 
    end = pd.Timestamp.now(), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
    limit = 100, # upper limit of number of data points
    feed = DataFeed.SIP
    )
quotes = stock_client.get_stock_quotes(quotes_request_params).df


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

