import pandas as pd
import numpy as np
import math, time, random
import datetime as dt
import os
import concurrent.futures
from typing import List
from itertools import repeat
from dateutil.relativedelta import relativedelta

import logging
import logging.handlers

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from twilio.rest import Client


from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderStatus, QueryOrderStatus, OrderClass, OrderType
from alpaca.trading.requests import GetCalendarRequest, GetAssetsRequest, GetOrdersRequest, MarketOrderRequest, LimitOrderRequest, StopLossRequest, TakeProfitRequest, TrailingStopOrderRequest
from alpaca.data.requests import StockLatestQuoteRequest, StockTradesRequest, StockQuotesRequest, StockBarsRequest, StockSnapshotRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed, Exchange

from alpaca.trading.client import TradingClient
from alpaca.data import StockHistoricalDataClient
from alpaca.broker.client import BrokerClient


sender_address = os.environ['sender_address']
receiver_address =os.environ['receiver_address']
smtp_handler = logging.handlers.SMTPHandler(
    mailhost=("smtp.gmail.com", 587),
    fromaddr=sender_address,
    toaddrs=[receiver_address],
    subject="Alpaca Error Log Message",
    credentials=(sender_address, os.environ['EMAIL_PASS']),
    secure=()
)
smtp_handler.setLevel(logging.ERROR)  # Messages with level ERROR and above (including CRITICAL) will trigger an email
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Messages with level INFO and above (including WARNING, ERROR, CRITICAL) will be output to the console

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level for the logger
logger.addHandler(smtp_handler)
logger.addHandler(console_handler)

# logger.info("This is an info message, will appear in console")
# logger.error("This is an error message, will be sent via email")




class MyAlpaca:

    def __init__(self, key, secret, strategy_name = 'Paper Testing', max_wait_time=30):
        self.trading_client = TradingClient(key, secret)
        self.stock_client = StockHistoricalDataClient(key, secret)
        self.broker_client = BrokerClient(key, secret,sandbox=False,api_version="v2")

        # Get our account information.
        account = self.trading_client.get_account()
        if account.trading_blocked:
            logger.error('Account is currently restricted from trading.')

        # variables:
        self.SimpleStopLoss = 0.05
        self.SimpleTakeProfit = 0.2
        self.SimpleLimitBuy = 0.99 # how limit price should be different from current ask
        self.max_wait_time = max_wait_time # seconds to wait for order execution

        self.strategy_name = strategy_name


    def quotes(self, symbols):
        return self.stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=symbols,feed=DataFeed.SIP))


    def buypower(self):
        return float(self.trading_client.get_account().buying_power)


    def get_ok_alpaca_stocks(self, spread_limit = 0.01):
        try:
            assets = self.trading_client.get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status= AssetStatus.ACTIVE))
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
                                and asset.easy_to_borrow # Alpaca currently uses its clearing firms ‘Easy To Borrow’ list and assumes everything else is Hard To Borrow. 
                                and asset.maintenance_margin_requirement == 30
                                and not (any(ex_string in asset.name for ex_string in exclude_strings))]

            latest_multisymbol_quotes = self.quotes(assets_in_scope)
            rows = []
            # TODO: check the bidask repo: https://github.com/eguidotti/bidask/tree/main/python
            for symbol, info in latest_multisymbol_quotes.items():
                try:
                    spread = (info.ask_price - info.bid_price) / info.ask_price
                    rows.append({'symbol': symbol, 'spread': spread})
                except:
                    continue
            df_spreads = pd.DataFrame(rows)
            # without filtering for spread it will be around 2500 symbols
            # spread_limit = 0.01 should deliver 550 symbols; 0.002 - around 50 symbols7
            return df_spreads[df_spreads.spread < spread_limit].symbol.unique().tolist()
        
        except Exception as e:
            logger.error(f"Error in get_ok_alpaca_stocks: {e}")
            return []  # return an empty list or handle it as needed


    def get_tkrs_snapshot_df(self,tickers):
        try:        
            clock = self.trading_client.get_clock()
            today = clock.timestamp
            snap = self.stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=tickers, feed = DataFeed.SIP))
            snapshot_data = {stock: [
                                    snapshot.latest_trade.timestamp,                        
                                    snapshot.latest_trade.price, 
                                    snapshot.daily_bar.open,
                                    snapshot.daily_bar.close,
                                    snapshot.previous_daily_bar.close,
                                    ]
                            for stock, snapshot in snap.items() if snapshot and snapshot.daily_bar and snapshot.previous_daily_bar
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

            return snapshot_df

        except Exception as e:
            logger.error(f"Error in get_tkrs_snapshot_df: {e}")
            return pd.DataFrame()  # return an empty DataFrame or handle it as neede


    def get_history(self, symbols: List[str], periods: int, FrameLength: int = 15, frame: str = 'min', num_threads: int = 4, chunk_size: int = 200):
        '''
        data = alpaca_instance.get_history(symbols=scope,periods=570) # 570 = 15min intervals 26 intervals per day = 1 month of data
        data = alpaca_instance.get_history(symbols=scope,periods=500,FrameLength=1,frame='day') # daily data
        '''
        # Type checks
        if not isinstance(symbols, list) or not all(isinstance(s, str) for s in symbols):
            raise TypeError("symbols must be a list of strings")
        if not isinstance(periods, int):
            raise TypeError("period must be an integer")
        if not isinstance(FrameLength, int):
            raise TypeError("FrameLength must be an integer")
        
        # Check for allowed TimeFrame values
        if frame not in ['day','hour','min']:
            raise ValueError("frame must be one of 'day','hour','min'")

        if frame == 'day':
            InternalFrame = TimeFrameUnit.Day
            divider = 1
        elif frame == 'hour':
            InternalFrame = TimeFrameUnit.Hour
            divider = 7
        elif frame == 'min':
            InternalFrame = TimeFrameUnit.Minute
            divider = 7*60
      
        def chunk_symbols(symbols, chunk_size):
            for i in range(0, len(symbols), chunk_size):
                yield symbols[i:i + chunk_size]

        def fetch_data_for_chunk(symbols_chunk, start_day, end_day, frame):
            bars_request_params = StockBarsRequest(
                symbol_or_symbols=symbols_chunk,
                start=start_day, end=end_day,
                timeframe=TimeFrame(FrameLength, InternalFrame), # e.g. 10 Minutes
                adjustment=Adjustment.ALL,
                feed=DataFeed.SIP
            )
            return self.stock_client.get_stock_bars(bars_request_params).df.reset_index()

        try:

            clock = self.trading_client.get_clock()
            end_day = clock.timestamp
            start_day = end_day.date() - relativedelta(days=max(1,(FrameLength*periods)//divider))

            symbols_chunks = chunk_symbols(symbols, chunk_size)

            dataframes = []
            logger.info(f'Starting concurrent download of {frame} data across {len(symbols)} tickers...')
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                for df in executor.map(fetch_data_for_chunk, symbols_chunks, repeat(start_day), repeat(end_day), repeat(frame)):
                    dataframes.append(df)

            data_df = pd.concat(dataframes, ignore_index=True)
            data_df.timestamp = data_df.timestamp.dt.tz_convert('America/New_York').dt.tz_localize(None) # Convert to market time and remove +00:00
            if frame != 'day':
                data_df = data_df[data_df['timestamp'].dt.time.between(pd.to_datetime('09:30:00').time(), pd.to_datetime('16:00:00').time())] # keep only market hours
            

            logger.info(f'Done downloading {frame} data. {len(data_df)} rows collected.')
            return data_df.reset_index(drop=True)

        except Exception as e:
            logger.error(f"Error in get_history: {e}")
            raise


    def add_columns(self, df):
        # Calculate span
        interval = (df['timestamp'].iloc[1] - df['timestamp'].iloc[0]).total_seconds() / 60
        intervals_per_day = 1 if interval == 1440 else (6.5 * 60) / interval # Number of intervals per day = Trading hours in a day / interval in hours
        intervals_num = int(df.groupby('symbol')['close'].count().median()) # there could be symbols with small history: we take the most often length (median)
        dayz = intervals_num//intervals_per_day
        span = int(min(dayz//2, 3) * intervals_per_day)
        weekperiod = int(min(intervals_num,5 * intervals_per_day))

        df['ema'] = df.groupby('symbol')['close'].transform(lambda x: x.ewm(span=span, adjust=False).mean())
        df['ret1w'] = df.groupby("symbol")["close"].pct_change(weekperiod)
        # df['logret'] = df.groupby('symbol')['close'].transform(lambda x: np.log(x / x.shift(1))) # Calculate log returns
        df['logret'] = df.groupby('symbol')['close'].pct_change().apply(np.log1p)

        df['volat'] = df.groupby('symbol')['logret'].transform(lambda x: x.rolling(window=span).std()) # calc volat without annualizing as we need just to compare


        # Calculate the direction of the price change: +1 for an increase, -1 for a decrease, 0 for no change.
        df['direction'] = df['logret'].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)

        def calculate_cumulative_streaks(series):
            streak = 0
            last_direction = 0
            streaks = []
            for value in series:
                if value == 0: # If no change in price, ...
                    streaks.append(streak) # .. add the current streak value
                elif value == last_direction: # If the direction is the same as the last one, ...
                    streak += value # .. # increment or decrement the streak.
                    streaks.append(streak)
                else: # If the direction changes, ..
                    streak = value # ... reset the streak to the current direction
                    streaks.append(streak)
                    last_direction = value
            return streaks

        # Applying the function to each group
        df['streak'] = df.groupby('symbol')['direction'].transform(calculate_cumulative_streaks)
        df.drop(columns=['direction'], inplace=True)        

        return df


    def check_trading_day(self):

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

        clock = self.trading_client.get_clock()
        current_time = clock.timestamp
        current_date = current_time.date()

        if clock.is_open:
            result["market_status"] = "open"
            open_time = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
            result["time_since_market_open"] = round((current_time - open_time).total_seconds() / 60,1)
            result["time_till_market_close"] = round((clock.next_close - current_time).total_seconds() / 60,1)
        else:
            calendar = self.trading_client.get_calendar(GetCalendarRequest(start=(current_date + pd.DateOffset(days=-5)), end=current_date))   
            result["time_since_market_close"] = round((current_time.replace(tzinfo=None) - calendar[-1].close).total_seconds() / 60,1) # calendar[-1] - latest trading day: today or earlier
            result["time_till_market_open"] = round((clock.next_open - current_time).total_seconds() / 60,1)


        first_of_month = current_date.replace(day=1)
        last_of_month = (current_date.replace(day=1) + pd.DateOffset(months=1)+pd.DateOffset(days=-1))
        calendar = self.trading_client.get_calendar(GetCalendarRequest(start=first_of_month, end=last_of_month))
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

        return result
    

    def get_atr_stoploss(self):
        raise NotImplementedError("This method hasn't been implemented yet.")


    def generate_coid(self):
        # generates client order id using strategy name and time
        timestring = str(int(time.mktime(self.trading_client.get_clock().timestamp.timetuple())))
        coid = self.strategy_name + "_" + timestring + "_" + str(random.randint(1, 1000))
        return coid


    def submit_market_order(self, ticker, money = 0, quantity = 0, side = OrderSide.BUY, orderclass = 'oto'):
        
        try:
            latest_quote = self.quotes([ticker])
            start_buying_power = self.buypower()            
            if quantity == 0: # the order was specified with amount of money to be spent
                if money <= 0:
                    logger.warning(f'No money or num of shares specified for {ticker}')
                    return 'Error'
                if money > start_buying_power:
                    logger.warning(f'Not enough buying power for {ticker}')
                    return 'Error'
                quantity = int(money//latest_quote[ticker].ask_price) # deriving qty

            if orderclass == 'oto': # OTO is needed to "attach" the TP or/and SL
                take_profit_price = round(latest_quote[ticker].bid_price*(1+self.SimpleTakeProfit),2)
                # for the stop loss we will go by periodically calculating the level (not submitting it beforehand)
                market_order_data = MarketOrderRequest(
                    symbol = ticker, 
                    qty = quantity, 
                    side = side, 
                    client_order_id = self.generate_coid(),
                    time_in_force = TimeInForce.GTC, 
                    take_profit = TakeProfitRequest(limit_price=take_profit_price, side = OrderSide.SELL), 
                    order_class = OrderClass.OTO, 
                    )
            elif orderclass == 'bracket': # BRACKET order (i.e. has SL and TP)
                limit_price_target = round(latest_quote[ticker].bid_price*self.SimpleLimitBuy,2)
                stop_loss_price = round(limit_price_target*(1-self.SimpleStopLoss),2)
                take_profit_price = round(limit_price_target*(1+self.SimpleTakeProfit),2)

                market_order_data = MarketOrderRequest(
                    symbol = ticker, 
                    qty = quantity, 
                    side = side, 
                    client_order_id = self.generate_coid(),
                    time_in_force = TimeInForce.GTC, 
                    take_profit = TakeProfitRequest(limit_price=take_profit_price, side = OrderSide.SELL), 
                    stop_loss= StopLossRequest(stop_price=stop_loss_price, side = OrderSide.SELL), 
                    order_class = OrderClass.BRACKET
                    )
            elif orderclass == 'simple': # Simple order without TP or SL
                market_order_data = MarketOrderRequest(
                    symbol = ticker, 
                    qty = quantity, 
                    side = side, 
                    client_order_id = self.generate_coid(),
                    time_in_force = TimeInForce.GTC, 
                    order_class = OrderClass.SIMPLE,
                    )
            else: 
                logger.warning(f'Wrong order class for {ticker}. Possible options: oto, bracket, simple.')
                return 'Error'
            market_order = self.trading_client.submit_order(order_data=market_order_data)
            status = None
            msg = ""
            start_time = time.time()
            while True:
                my_order = self.trading_client.get_order_by_id(market_order.id)
                status = my_order.status

                if status == 'filled': # order is executed
                    break
                
                wait_time = self.max_wait_time
                msg = f"Timeout reached for order {ticker}"
                if not self.trading_client.get_clock().is_open:
                    wait_time = 0 # there is no sense to wait when market is closed
                    msg = f'Market is closed. Order for {ticker} will be executed on next on-market-open.'
                if time.time() - start_time > wait_time:
                    logger.warning(msg)
                    break

                time.sleep(1)

            if status == 'filled':
                message = f'{self.strategy_name}: {my_order.side} - {my_order.symbol} - {round(float(my_order.filled_qty)*float(my_order.filled_avg_price),0)} - {self.buypower()}'
                self.sent_alpaca_email('Market Order Execution: ', message) # sms + email
                return "Done"
            else:
                self.sent_alpaca_email('Market Order Execution timed out: ', msg) # sms + email
                return "Error"
        except Exception as e:
            logger.error(f"Error in submit_bracket_order: {e}")
            return "Error"


    def close_selected_positions(self, sell_dictionary):
        start_buying_power = self.buypower()
        open_orders = self.trading_client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN))
        positions = self.trading_client.get_all_positions()

        for ticker, qty_to_sell in sell_dictionary.items():
            if isinstance(qty_to_sell, str):
                logger.warning(f"Skipping {ticker}: quantity is a string ({qty_to_sell})")
                continue
            if qty_to_sell <= 0:
                logger.warning(f"Skipping {ticker}: invalid quantity {qty_to_sell}")
                continue
            position = next((p for p in positions if p.symbol == ticker), None)
            if position: # case when we are selling what we own
                qty_available = position.qty_available # how many stocks are NOT frozen in open orders
                qty_missing = qty_to_sell - qty_available
                while qty_missing > 0: # qty_available is insufficient: need to adjust open orders
                    ticker_orders = [o for o in open_orders if o.symbol == ticker] # open orders for current ticker
                    for order in ticker_orders:
                        if order.qty >= qty_missing: # current open order qty is big enough to free-up stocks for selling
                            new_qty = order.qty - qty_missing
                            self.trading_client.replace_order(order.id, qty=new_qty) # Replace the order with reduced quantity
                            time.sleep(1)
                            qty_missing -= qty_missing
                            break
                        else: # current open order qty is NOT enough to free-up stocks for selling
                            self.trading_client.cancel_order_by_id(order_id=order.id) # cancel the order to free up the quantity
                            time.sleep(1)
                            qty_missing -= order.qty
                    # now the qty_available should be sufficient
                self.submit_market_order(ticker = ticker, quantity = qty_to_sell, side = OrderSide.SELL, orderclass = 'simple')
            else:
                msg = f"Skipping {ticker}: not in existing position."
                logger.warning(msg)
                continue

        self.sent_alpaca_email('Buying Power',f'After selling, buying power increased by {self.buypower() - start_buying_power}')
        return "sold"


    def close_pl_positions(self):
        clock = self.trading_client.get_clock()
        if clock.is_open:
            try:
                positions = self.trading_client.get_all_positions()
                positions_dict= {position.symbol: int(position.qty) 
                                 for position in positions 
                                 if float(position.unrealized_plpc) < -self.SimpleStopLoss
                                 or float(position.unrealized_plpc) > self.SimpleTakeProfit}
                logger.info(positions_dict)
                if positions_dict:
                    self.close_selected_positions(positions_dict)
            except Exception as e:
                self.sent_alpaca_email('Error in checking pl: ',e) # sms + email
                return "error"
        else:
            return 'done'


    def get_day_summary(self, hoursago = 24):
        try:
            if hoursago <= 0:
                logger.error(f"Number of hours should be positive.")
                return "Error"
            account = self.trading_client.get_account()
            # Check our current balance vs. our balance at the last market close
            balance_change = round(float(account.equity) - float(account.last_equity),0)
            date_filter = (pd.Timestamp.now()- pd.Timedelta(hoursago, "hours")).floor(freq='min') # orders for today # orders for today
            orders = self.trading_client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.CLOSED,after = date_filter))
            today_date = pd.Timestamp.today().date()  # Call the method to get today's date
            formatted_date = today_date.strftime('%d.%b')  # Format the date
            if orders:
                orders_dicts = []
                for entry in orders:
                    entry_dict = {}  # Create a new dictionary for each entry
                    for attribute, value in vars(entry).items():
                        entry_dict[attribute] = value  # Populate the dictionary
                    orders_dicts.append(entry_dict)  # Append the populated dictionary to the list
                df = pd.DataFrame(orders_dicts)
                df = df[['filled_at','symbol','filled_qty','order_type','side']].copy()
                df['filled_at'] = df['filled_at'].dt.tz_localize(None).dt.strftime('%H:%M')

                msg = (
                        f"Summary for last {hoursago} hours ending today on {pd.Timestamp.today().day_name()}, {formatted_date}:\n\n"
                        f"Balance change: {balance_change}\n\n"
                        f"Total {len(orders)} were executed:\n"
                        f"{df.to_string(index=False)}\n"
                    )
            else:
                msg = (
                        f"Summary for last {hoursago} hours ending today on {pd.Timestamp.today().day_name()}, {formatted_date}:\n\n"
                        f"Balance change: {balance_change}\n\n"
                        f"No orders executed for the period."
                    )
            self.sent_alpaca_email('Daily summary', msg)
            return "sent-ok"
        except Exception as e:
            msg = f"Error in sending daily summary: {e}"
            self.sent_alpaca_email('Error daily summary', msg)
            return "Error"


    def sent_alpaca_email(self, mail_subject, mail_content):
        try:
            logger.info(mail_content)
            email_sent = False
            message = MIMEMultipart() # setup the MIME
            message['From'] = 'Alpaca - ' + self.strategy_name
            message['To'] = receiver_address
            message['Subject'] = mail_subject   #The subject line
            message.attach(MIMEText(mail_content, 'plain')) # body and the attachments for the mail

            #Create SMTP session for sending the mail
            session = smtplib.SMTP('smtp.gmail.com', 587) # use gmail with port
            session.starttls() # enable security
            session.login(sender_address, os.environ['EMAIL_PASS']) # login with mail_id and password
            text = message.as_string()
            session.sendmail(sender_address, receiver_address, text)
            session.quit()
            email_sent = True
            return email_sent
        except Exception as e:
            logger.error(f"Error in sent_alpaca_email: {e}")
            return False  # return False or handle the exception as needed


    # self.twilio_client = Client(Twilio_account_SID, Twilio_token)#  Send the message
    # def inform(self, message_to_send):
    #     try:
    #         message = self.twilio_client.messages.create(
    #                                 body=message_to_send,
    #                                 from_=Twilio_phone,
    #                                 to=Reciep_Phone
    #                             )
    #     except Exception as e:
    #         logger.error(f"Error in inform: {e}")
    #         # handle the error, maybe retry or log the failure

