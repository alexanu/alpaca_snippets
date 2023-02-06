# New SDK: https://github.com/alpacahq/alpaca-py

import Universes
from Alpaca_config import *

import pandas as pd
import time
import datetime as dt
import random

import alpaca

from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderStatus, QueryOrderStatus, CorporateActionType, CorporateActionSubType
from alpaca.trading.requests import GetCalendarRequest, GetAssetsRequest, GetOrdersRequest, MarketOrderRequest, LimitOrderRequest, StopLossRequest, TakeProfitRequest, TrailingStopOrderRequest, GetPortfolioHistoryRequest, GetCorporateAnnouncementsRequest
from alpaca.data.requests import StockLatestQuoteRequest, StockTradesRequest, StockQuotesRequest, StockBarsRequest, StockSnapshotRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed, Exchange

from alpaca.trading.client import TradingClient
from alpaca.data import StockHistoricalDataClient
from alpaca.broker.client import BrokerClient
trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER) # dir(trading_client)
stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)
broker_client = BrokerClient(API_KEY_PAPER,API_SECRET_PAPER,sandbox=False,api_version="v2")



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


        # Portfolio history
            account_id = trading_client.get_account().id
            data_pf = broker_client.get_portfolio_history_for_account(account_id)

            data_pf = broker_client.get_portfolio_history_for_account(
                                        trading_client.get_account().id,
                                        GetPortfolioHistoryRequest(period='2W', # <number> + <unit>: D, W, M, A
                                                                    timeframe='5Min', # 1Min, 5Min, 15Min, 1H, 1D
                                                                    extended_hours = True)
                                                        )
            data_pf['lagged_equity'] = data_pf.equity.shift(1)
            data_pf['pct_change'] = data_pf.profit_loss / data_pf.lagged_equity
            # filter by date if desired
            dateFilter = '2021-01-19'
            data = data_pf[data_pf.timestamp >= dateFilter].reset_index(drop=True)




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
                    # extended_hours=True & type='limit' & time_in_force=TimeInForce.DAY:
                            # A limit orders with a limit price that significantly exceeds the current market price will be rejected
                            # Any other order types, including market orders, will be rejected
                            # extended hours order must be DAY limit orders
                    # Your extended hour order will be processed and filled immediately.
            # Orders not eligible for extended hours 
            #       submitted between 4:00pm - 7:00pm ET will be rejected.
            #       submitted after 7:00pm ET will be queued and eligible for execution at the time of the next market open

        dir(TimeInForce)
        # Time in force: https://alpaca.markets/docs/trading/orders/#time-in-force
            '''
            gtc - good-till-cancelled
            day - eligible for execution only on the day it is live (9:30am - 4:00pm ET)
            opg - use it to submit “market on open” (MOO) and “limit on open” (LOO) orders
            cls - use it to submit “market on close” (MOC) and “limit on close” (LOC) orders
            ioc - Immediate Or Cancel (IOC) - all or part of the order to be executed immediately
            fok - Fill or Kill (FOK)
            '''

        strategy_name = "Break_out_10min"
        coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
        ticker = 'SPY'
        ticker = random.choice(Universes.Spiders)
        limit_buy = 0.99 # how limit price should be different from current ask
        stop_loss = trail_sl = 5 # in %, easy stop-loss, I should use ATR() like in zorro probably
        take_profit = 20 # in %        
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

        # bracket order
            market_order = MarketOrderRequest(
                symbol = symbol, 
                qty = quantity, 
                side = OrderSide.BUY, 
                time_in_force = TimeInForce.GTC, 
                take_profit = TakeProfitRequest(limit_price=limit_price_target*(1+take_profit/100), side = OrderSide.SELL), 
                stop_loss= StopLossRequest(stop_price=limit_price_target*(1-take_profit/100), side = OrderSide.SELL), 
                order_class = OrderClass.BRACKET
                )



        # Trailing order: https://alpaca.markets/docs/trading/orders/#trailing-stop-orders
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
            limit_order_data = LimitOrderRequest(
                symbol=ticker,
                limit_price=limit_price_target,
                qty=1,
                side=OrderSide.BUY,
                extended_hours = True,
                client_order_id = coid,
                stop_loss = StopLossRequest(stop_price=limit_price_target*(1-stop_loss)),
                time_in_force=TimeInForce.DAY) # dir(TimeInForce), extended hours order must be DAY limit orders
            limit_order = trading_client.submit_order(order_data=limit_order_data)



        # close position
        trading_client.close_position('SPY') 
        trading_client.close_all_positions(cancel_orders=True) # closes all position AND also cancels all open orders

        # Take profit
            positions = trading_client.get_all_positions()
            for position in positions:
                profit = float(position.unrealized_pl)
                percentChange = (profit/float(position.cost_basis)) * 100
                if (percentChange > 5):
                    print(f"Selling {position.qty} shares of {position.symbol}")
                    trading_client.submit_order(MarketOrderRequest(symbol=position.symbol,qty=position.qty,side=OrderSide.SELL,client_order_id = coid,time_in_force=TimeInForce.OPG))
            
        # cancel orders
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

        dir(OrderStatus) # 'ACCEPTED', 'ACCEPTED_FOR_BIDDING', 'CALCULATED', 'CANCELED', 
                        # 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED', 'REPLACED', 'FILLED', 'NEW', 'PARTIALLY_FILLED', 
                        # 'PENDING_CANCEL', 'PENDING_NEW', 'PENDING_REPLACE',  'STOPPED', 'SUSPENDED'
            # full list of statuses: https://alpaca.markets/docs/trading-on-alpaca/orders/#order-lifecycle
                # Updates on orders states at Alpaca will be sent over the streaming interface


        date_filter = (pd.Timestamp.now()- pd.Timedelta(30, "days")).floor(freq='S') # orders for last 30 days
        date_filter = (pd.Timestamp.now()- pd.Timedelta(5, "minutes")).floor(freq='S'), # orders for last 5 minutes
        date_filter = (pd.Timestamp.now()- pd.Timedelta(30, "days")).floor(freq='S').isoformat() # isoformat also works

    # extract all orders to df
        request_params = GetOrdersRequest(status=QueryOrderStatus.OPEN, # Not the same as OrderStatus
                                        after = date_filter,
                                        # side=OrderSide.BUY, # optional
                                        # symbols = ['SPY','QQQ'], # optional
                                        )
        orders = trading_client.get_orders(filter=request_params)
        orders_df = pd.concat((pd.DataFrame(order).set_index(0) for order in orders),axis=1).T
        columns_to_convert = ['created_at','updated_at','submitted_at','filled_at','expired_at','canceled_at','failed_at','replaced_at']
        for column in columns_to_convert:
            try:
                orders_df[column] = orders_df[column].dt.ceil(freq='s').dt.tz_convert('Europe/Berlin').dt.tz_localize(None)
                                                    # ro              # convert from UTC to local time    # remove time zone
            except: # this we need as empty columns could give error
                pass
        orders_df.to_excel('orders.xlsx')

    # filter by client id
        orders = trading_client.get_orders()
        search_string = 'esting'
        filtered_orders = [order for order in orders if search_string in order.client_order_id]
        filtered_orders_df = pd.concat((pd.DataFrame(order).set_index(0) for order in orders if search_string in order.client_order_id),axis=1).T

        # So these 2 below are not really needed. Or maybe to check the Order Status of a recently sent order, which ID is still in variable
            trading_client.get_order_by_client_id()
            trading_client.get_order_by_id()

    SPY_orders = [o for o in orders if o.symbol == 'SPY']
    open_sell_symbols = {order.symbol for order in orders if order.side == "buy" and order.status == "canceled" and order.filled_at == None}


    # current positions
        positions = trading_client.get_all_positions()
        positions =[]
        if positions:
            print("hi")
        positions_symbols_set = {p.symbol for p in positions}
        [print(f"{p.symbol} with profit of {p.unrealized_pl}",end="; ") for p in positions]


        stop_loss = -0.05
        positions_symbols_to_close = [p.symbol for p in positions if float(p.unrealized_plpc)<stop_loss]

        symbol = 'SPY'
        positions_dict = {position.symbol: position.qty for position in positions}
        if symbol in positions_dict:
            print('Yes')



        try:
            position = trading_client.get_open_position('XLF').qty
        except: # No position exists
            position = 0

        positions[1]._get_value
        positions_df = pd.DataFrame([p for p in positions])

        positions_df = pd.concat((pd.DataFrame(position).set_index(0) for position in positions),axis=1)
        positions_df = positions_df.T.apply(pd.to_numeric, errors='ignore').T # convert strings to numeric
        positions_df[['unrealized_plpc','unrealized_intraday_plpc','change_today']] = positions_df[['unrealized_plpc','unrealized_intraday_plpc','change_today']].applymap("{0:.2f}".format)


        positions = trading_client.get_all_positions() 
        positions_list = []
        for position in positions:
            position_dict = dict(position)
            positions_list.append(dict(position))

        positions_df = pd.DataFrame(positions_list)
        as_numeric_columns = ['avg_entry_price', 'qty', 'market_value', 'cost_basis', 'unrealized_pl', 'unrealized_plpc', 'unrealized_intraday_pl', 'unrealized_intraday_plpc', 'current_price', 'lastday_price', 'change_today']
        positions_df[as_numeric_columns] = positions_df[as_numeric_columns].astype(float).round(3)

        
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
    alpaca_calendar[0]


    now = pd.Timestamp.today() + pd.offsets.Day(-1)
    MonthEnd = (now + pd.offsets.BusinessMonthEnd(normalize=True)).strftime("%Y-%m-%d")
    trading_till_moe = trading_client.get_calendar(GetCalendarRequest(start=now.strftime("%Y-%m-%d"), end=MonthEnd))
    len(trading_till_moe)

    weird_close_times = [day for day in trading_till_moe if day.close.hour != 16] # check if in the upcoming days exchange closes earlier than usual

    pd.Timestamp(trading_till_moe[0].close).strftime("%b %d, %H:%M") # Close time ET today 
    pd.Timestamp(trading_till_moe[0].close).tz_localize('US/Eastern').tz_convert('UTC')

    clock = trading_client.get_clock()

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
            ca_types=['MERGER'],
            since = '2022-11-11', # 90 days
            until = '2022-12-11', # 90 days
            # date_type = '', # declaration_date, ex_date, record_date, payable_date
        )) # error

    dir(CorporateActionType)
    '''
    Details: https://alpaca.markets/docs/api-references/trading-api/corporate-actions-announcements/

    DIVIDEND = "dividend"
    CASH = "cash"
    STOCK = "stock"

    MERGER = "merger"
    MERGER_UPDATE = "merger_update"
    MERGER_COMPLETION = "merger_completion"

    SPINOFF = "spinoff"

    SPLIT = "split"
    STOCK_SPLIT = "stock_split"
    UNIT_SPLIT = "unit_split"
    REVERSE_SPLIT = "reverse_split"

    RECAPITALIZATION = "recapitalization"
    '''

    ca_types: List[CorporateActionType]
    since: date
    until: date
    symbol: Optional[str]
    cusip: Optional[str]
    date_type: Optional[CorporateActionDateType]

    trading_client.get_corporate_announcement_by_id() # error


# Streaming Trade Updates
    from alpaca.trading.stream import TradingStream
    trading_stream = TradingStream(API_KEY_PAPER,API_SECRET_PAPER, paper=True)
    async def update_handler(data):     # trade updates will arrive in our async handle
        print(data)
    trading_stream.subscribe_trade_updates(update_handler)
    trading_stream.run()


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


        # Bars

            # for 1 symbol
                bars_request_params = StockBarsRequest(
                    symbol_or_symbols=['SPY'], 
                    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(2, "days")).floor(freq='S'), # T - for minutes, H - for hours
                    end = pd.Timestamp.now(), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
                    # limit = 100, # upper limit of number of data points
                    timeframe=TimeFrame.Minute, # 'Day', 'Hour', 'Minute', 'Month', 'Week'
                    adjustment= Adjustment.RAW, # SPLIT, DIVIDEND, ALL
                    feed = DataFeed.SIP
                    )
                hist_bars = stock_client.get_stock_bars(bars_request_params).df.droplevel(level=0) 
                                                                                # drop level is needed as 1st it appears with multiindex with symbol
                                                                                # if in request there were >1 symbol, this should NOT be used
                hist_bars.index = hist_bars.index.tz_convert('America/New_York').tz_localize(None)
                                                    # Convert to market time for easier reading
                                                                                # remove +00:00 from datetime


                today = trading_client.get_clock().timestamp
                previous_day = today - pd.Timedelta('1D')
                previous_day_10 = today - pd.Timedelta('10D')
                stock = 'SPY'
                bars_request_params = StockBarsRequest(symbol_or_symbols=stock, start = previous_day_10, end = previous_day, timeframe=TimeFrame.Day, adjustment= Adjustment.RAW,feed = DataFeed.SIP)
                bar_data = stock_client.get_stock_bars(bars_request_params).df.droplevel(level=0) # drop level is needed as 1st it appears with multiindex with symbol

                bars_request_params = StockBarsRequest(symbol_or_symbols=stock, limit = 10, end = previous_day, timeframe=TimeFrame.Day, adjustment= Adjustment.RAW,feed = DataFeed.SIP)
                bar_data = stock_client.get_stock_bars(bars_request_params).df.droplevel(level=0) # drop level is needed as 1st it appears with multiindex with symbol



            # for many symbols
                bars_request_params = StockBarsRequest(
                    symbol_or_symbols=Universes.Spiders, 
                    start = (pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(2, "days")).floor(freq='S'), # T - for minutes, H - for hours
                    end = pd.Timestamp.now(), # do I need to convert to ET? (tz="US/Eastern") or UTC? pd.Timestamp.utcnow()
                    # limit = 100, # upper limit of number of data points
                    timeframe=TimeFrame.Minute, # 'Day', 'Hour', 'Minute', 'Month', 'Week'
                    adjustment= Adjustment.RAW, # SPLIT, DIVIDEND, ALL
                    feed = DataFeed.SIP
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

# Latest Bar

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

