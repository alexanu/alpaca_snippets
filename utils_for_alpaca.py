import pandas as pd
import numpy as np
import math, time, random
import datetime as dt
import os
from github import Github
from github.GithubException import UnknownObjectException, GithubException
import io
import concurrent.futures
from typing import List, Dict
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


max_positions_allowed = 15
def num_of_positions_ok(key, secret):
    positions = TradingClient(key, secret).get_all_positions()    
    logger.info(f'Currently there {len(positions)} open positions, while {max_positions_allowed} are max allowed')
    return max_positions_allowed > len(positions)




def _write_df_to_github(repo, file_path, df, sha=None, commit_message="Updated CSV with new data"):
    csv_modified = df.to_csv(index=False)
    try:
        if sha:
            # Update the existing file
            repo.update_file(file_path, commit_message, csv_modified, sha)
        else:
            # Create a new file
            repo.create_file(file_path, commit_message, csv_modified)
        logger.info(f'Successfully saved data to {file_path}')
    except GithubException as e:
        raise RuntimeError(f"Failed to write the file to GitHub: {str(e)}")


def _read_csv_from_github(repo, file_path):
    try:
        contents = repo.get_contents(file_path)
        df = pd.read_csv(
            io.StringIO(contents.decoded_content.decode()),
            delimiter=";"
            )
        logger.info(f'Successfully read {file_path} from GitHub')
        return df, contents.sha
    except UnknownObjectException:
        # File does not exist
        return pd.DataFrame(), None
    except GithubException as e:
        raise RuntimeError(f"Failed to read the file {file_path} from GitHub: {str(e)}")



def _read_strategy_config(strategy):
    try:
        # reading strategy metadata from github
        repository = Github(os.getenv('github_strat_token')).get_user().get_repo(os.getenv('dedicated_repo'))
        strategy_config_file = repository.get_contents(os.getenv('gh_csv_strategy_config'))
        strategy_config_dict = pd.read_csv(
            io.StringIO(strategy_config_file.decoded_content.decode()),
            skiprows=1,
            delimiter=";"
            ).set_index('StrategyName').transpose().to_dict()
        try:
            logger.info(f'Successfully read config for {strategy} strategy')
            return strategy_config_dict[strategy]
        except KeyError:
            logger.error(f"Strategy '{strategy}' not found in the strategy config dictionary.")
            return "Strategy not found"
    except Exception as e:
        logger.error(f"Error in reading strategy config from GH: {e}")
        return "Error"        


def create_dict_from_dataframe(df, value_column):
    """
    Create a dictionary from a dataframe where the keys are from the 'symbol' column
    and the values are from the specified value_column.
    I used this function when deriving weights in pf to buy.
    """
    if 'symbol' not in df.columns or value_column not in df.columns:
        raise ValueError("DataFrame must contain 'symbol' and the specified value column")
    
    return df.set_index('symbol')[value_column].to_dict()


class MyAlpaca:

    def __init__(self, key, secret, strategy_name = 'PaperTesting', max_wait_time=30):
        self.trading_client = TradingClient(key, secret)
        self.stock_client = StockHistoricalDataClient(key, secret)
        self.broker_client = BrokerClient(key, secret,sandbox=False,api_version="v2")

        # Get our account information.
        account = self.trading_client.get_account()
        if account.trading_blocked:
            logger.error('Account is currently restricted from trading.')

        # variables:
        strategy_config = _read_strategy_config(strategy_name)
        if strategy_config['SimpleStopLoss'] >= 0 and strategy_config['SimpleStopLoss'] < 100:
            self.SimpleStopLoss = strategy_config['SimpleStopLoss']/100
        else:
            self.SimpleStopLoss = 0.05

        if strategy_config['SimpleTakeProfit'] > 0:
            self.SimpleTakeProfit = strategy_config['SimpleTakeProfit']/100
        else:
            self.SimpleTakeProfit = 0.2

        if strategy_config['Allocated_Capital'] >= 0 and strategy_config['Allocated_Capital'] <= 100:
            self.alloc_capital_perc = strategy_config['Allocated_Capital']/100
            initiating_strat = ""
        else:
            self.alloc_capital_perc = 0 # not active strategy
            initiating_strat = " (but no capital is allocated to it)"

        self.SimpleLimitBuy = 0.99 # how limit price should be different from current ask
        self.max_wait_time = max_wait_time # seconds to wait for order execution

        self.universe = strategy_config['Universe']
        self.stocks_per_run = strategy_config['MaxNumberStocks']

        self.max_open_positions = max_positions_allowed # this is the limit for all strategies in total, so could be smth like super class
                                     # I should probably differentiate between ETFs and stocks
                                     # I should also probably think about limits for tickers from 1 sector...


        self.strategy_name = strategy_name
        self.min_investment_per_ticker = round(self.buypower()*0.005,0)
        self.max_investment_per_ticker = round(self.buypower()*0.05,0)
        self.max_holding_time = 40 # max num of days a stock could be in pf 
                                   #- as I always have a TP at 20% it means for 40 days the TP was not hit and I could be wasting a spot in my pf for another opportunity
                                   # ... as I also have limit for number of simultaneously opened positioned
        logger.info(f'Strategy {strategy_name} has been initialized{initiating_strat}')


    def quotes(self, symbols):
        return self.stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=symbols,feed=DataFeed.SIP))


    def buypower(self):
        '''buying power is 2x equity if 2000 < equity < 25000, and 4x equity if equity > 25000'''
        return float(self.trading_client.get_account().buying_power)


    def calculate_strategy_invested_volume(self):
        '''should show how much are currently invested with current strategy'''
        # TODO: implement
        return 0


    def strategy_buypower(self):
        dedicated_capital = self.alloc_capital_perc * self.buypower()
        invested_capital = self.calculate_strategy_invested_volume()
        return dedicated_capital-invested_capital


    def allocate_capital(self, amount: float, tickers: List[str]=None, method: str = "equal", manual_weights: Dict[str, float] = None) -> Dict[str, float]:

        if method == "manual" and manual_weights:
            if not all(isinstance(value, (int, float)) for value in manual_weights.values()):
                raise ValueError("All values in manual_weights must be numbers (int or float).")
            tickers = list(manual_weights.keys())
        elif not tickers:
            raise ValueError("Ticker list is empty")
        
        investment_distribution = {}

        if method == "equal":
            equal_amount = round(amount / len(tickers),0)
            investment_distribution = {ticker: min(equal_amount,self.max_investment_per_ticker) for ticker in tickers if equal_amount >= self.min_investment_per_ticker}

        elif method == "random":
            weights = np.random.dirichlet(np.ones(len(tickers)), size=1)[0]
                        # often used to generate random probability vectors, where the components are non-negative and sum to 1
            for ticker, weight in zip(tickers, weights):
                allocated_amount = amount * weight
                if allocated_amount >= self.min_investment_per_ticker:
                    investment_distribution[ticker] = min(round(allocated_amount,0),self.max_investment_per_ticker)

        elif method == "manual" and manual_weights:
            '''Converts dictionary of tickers with values into dictionary of tickers with weights'''
            min_value = min(manual_weights.values())
            if min_value < 0:
                adjusted_values = {ticker: value - min_value - min_value for ticker, value in manual_weights.items()}
            else:
                adjusted_values = manual_weights
            total_adjusted_value = sum(adjusted_values.values())
            # Normalize values to get weights
            weights = {ticker: adjusted_value / total_adjusted_value for ticker, adjusted_value in adjusted_values.items()}
            for ticker, weight in weights.items():
                allocated_amount = amount * weight
                if allocated_amount >= self.min_investment_per_ticker:
                    investment_distribution[ticker] = min(round(allocated_amount,0),self.max_investment_per_ticker)

        else:
            raise ValueError("Invalid method or missing manual weights")

        return investment_distribution


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


    def get_strategy_universe(self):

        strategy_tickers = {
            'SPY': ['SPY'],
            'Spiders': ['XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLY', 'XLU', 'XLV']
        }
        
        # Check if the strategy name is 'All', if so, call get_all_alpaca_stocks
        if self.universe == 'All':
            tickers_in_scope = self.get_ok_alpaca_stocks()
        else:
            # Return the list of tickers for the given strategy name
            # If the strategy name is not found, return an empty list
            tickers_in_scope = strategy_tickers.get(self.universe, [])
        
        logger.info(f'Strategy {self.strategy_name} has universe {self.universe} ({len(tickers_in_scope)} tickers)')
        return tickers_in_scope


    def is_positive(self,tickers=['SPY']):
        tickers_good = []
        snaps = self.stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=tickers, feed = DataFeed.SIP))
        for stock in snaps.keys():
            if snaps[stock].daily_bar.close/snaps[stock].daily_bar.open >1:
                tickers_good.append(stock)
        return tickers_good


    def get_tkrs_snapshot_df(self,tickers):
        try:        
            clock = self.trading_client.get_clock()
            today = clock.timestamp # ET date time
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
            logger.info(f'Stocks snapshot done. The df has {len(snapshot_df)} rows.')

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
        ''' ema, ret1w, logret, drawdown, volat, streaks'''
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

        # Define a function to calculate drawdown
        def calculate_drawdown(prices):
            max_prices = prices.cummax()
            drawdown = (prices - max_prices) / max_prices
            return drawdown

        # Apply the drawdown calculation
        df['drawdown'] = df.groupby('symbol')['close'].transform(calculate_drawdown)

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


    def update_database(self):

        DB_FILE = os.getenv('gh_csv_stocks_db')
        try:
            # Authenticate to GitHub and get the repo
            repo = Github(os.getenv('github_strat_token')).get_user().get_repo(os.getenv('dedicated_repo'))        
        except GithubException as e:
            raise RuntimeError(f"Failed to connect to GitHub or access the repository: {str(e)}")

        current_db = pd.DataFrame()
        current_db, sha = _read_csv_from_github(repo, DB_FILE)
        
        all_tickers = self.get_ok_alpaca_stocks(spread_limit = 0.02)
        df_6mohist = self.get_history(symbols=all_tickers, periods=14*22*6, FrameLength=30, frame = 'min')
        df_6mohist['time_period'] = df_6mohist['timestamp'].dt.strftime('%H:%M:%S')

        # calculating average 30min volume and trades and adding to the db
        try:
            logger.info(f'Calculating avg volume and trades for {len(all_tickers)} tickers')
            average_df  = df_6mohist.groupby(['symbol', 'time_period']).agg({'volume': 'mean', 'trade_count': 'mean'}).reset_index()
            pivot_df = average_df.pivot_table(index='symbol', columns='time_period', values=['volume', 'trade_count'])
            pivot_df.columns = [f'{val}_{col.replace(":", "")}' for val, col in pivot_df.columns]
            logger.info(pivot_df.head())
            if current_db.empty:
                updated_db = pivot_df
            else:
                updated_db = pd.merge(current_db, pivot_df, on='symbol', how='left')
            logger.info(updated_db.head())            
        except Exception as e:
            logger.error(f"Error in getting avg volumes: {e}")
            
        _write_df_to_github(repo, DB_FILE, updated_db, sha)





    def get_history_minute_single(self, symbol, start_time, window = 5, only_market = True):
        '''For a single ticker on window-minutes timeframe'''
        try:
            bars_request_params = StockBarsRequest(
                symbol_or_symbols=[symbol],
                start=start_time, end=self.trading_client.get_clock().timestamp,
                timeframe=TimeFrame(window, TimeFrameUnit.Minute),
                adjustment=Adjustment.ALL,
                feed=DataFeed.SIP
            )
            data_df = self.stock_client.get_stock_bars(bars_request_params).df.reset_index()
            logger.info(f'{len(data_df)} rows were downloaded for {symbol} to get current drawdown.')
            data_df.timestamp = data_df.timestamp.dt.tz_convert('America/New_York').dt.tz_localize(None) # Convert to market time and remove +00:00
            if only_market:
                data_df = data_df[data_df['timestamp'].dt.time.between(pd.to_datetime('09:30:00').time(), pd.to_datetime('16:00:00').time())] # keep only market hours

            return data_df

        except Exception as e:
            logger.error(f"Error in get_history: {e}")
            raise


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
        coid = self.strategy_name + "_" + timestring + str(random.randint(1, 1000)) # need to add some random in the end to avoid problem if orders are submitted in the same time
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
                # the idea is to always have high takeprofit in order to be able accidently profit from price jumps
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
            
            logger.info(f'Submiting {orderclass} {side.name} order for {quantity} of {ticker} (strategy: {self.strategy_name})')
            market_order = self.trading_client.submit_order(order_data=market_order_data)
            status = None
            msg = ""
            
            if self.trading_client.get_clock().is_open:
                start_time = time.time()
                while True:
                    my_order = self.trading_client.get_order_by_id(market_order.id)
                    if my_order.status == 'filled': # order is executed
                        break                    
                    if time.time() - start_time > self.max_wait_time:
                        msg = f"Despite market is open, the timeout was reached for order on {ticker} (strategy: {self.strategy_name})"
                        break
                    time.sleep(1)
            else:
                msg = f'Market is closed. Order for {ticker} will be executed on next on-market-open (strategy: {self.strategy_name}).'

            if status == 'filled':
                message = f'{self.strategy_name}: {my_order.side} - {my_order.symbol} - money: {round(float(my_order.filled_qty)*float(my_order.filled_avg_price),0)} - available buypower: {self.buypower()}'
                logger.info(f'Market Order Execution: {message}')
                return "Done"
            else:
                logger.warning(msg)
                return "Error"
        except Exception as e:
            logger.error(f"Error in submit_market_order(): {e}")
            return "Error"


    def open_selected_positions(self, buy_dictionary):
        start_buying_power = self.buypower()
        positions = self.trading_client.get_all_positions()

        # TODO: add step to check if any ticker from buy_dict was recently (idk... during last 7 days) sold from pf

        # TODO: shall we open a position despite a stock being at critical e.g. -4% drawdown? or this should be strategy-specific

        for ticker, tobe_invesment in buy_dictionary.items():
            if isinstance(tobe_invesment, str):
                logger.warning(f"Skipping {ticker}: amount is a string ({tobe_invesment})")
                continue
            if tobe_invesment <= 0:
                logger.warning(f"Skipping {ticker}: invalid investment volume {tobe_invesment}")
                continue
            position = next((p for p in positions if p.symbol == ticker), None)
            if position: # case when we are adding to already opened position
                current_investment = float(position.market_value) # size of current position
                investment = max(tobe_invesment - current_investment,0) # how much much to invest additionally
                investment = 0 # I decided not to buy additional stocks if there is a position already ...
                               # ... by doing this it will be easier to calculate strategy profitability and realize trailing stop
                if investment > 0:
                    logger.info(f'Amount to buy for {ticker} was reduced from {tobe_invesment} to {investment} as there is some investments in this ticker already')
                    self.submit_market_order(ticker = ticker, money = investment, side = OrderSide.BUY, orderclass = 'oto')
                else:
                    logger.info(f'Skipping buying {ticker} as the current investment is enough ({current_investment})')
            else: # no existing position in the ticker
                self.submit_market_order(ticker = ticker, money = tobe_invesment, side = OrderSide.BUY, orderclass = 'oto')

        self.sent_alpaca_email(f'{self.strategy_name}: strategy execution',f'Buying for strategy {self.strategy_name} has been done. Buying power decreased by {self.buypower() - start_buying_power}')
        return "bought"


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
                qty_available = int(position.qty_available) # how many stocks are NOT frozen in open orders
                qty_missing = qty_to_sell - qty_available
                while qty_missing > 0: # 
                    logger.info(f'Available quantity for {position.symbol} is insufficient: need to adjust open orders')
                    ticker_orders = [o for o in open_orders if o.symbol == ticker] # open orders for current ticker
                    for order in ticker_orders:
                        order_qty = int(order.qty)
                        if order_qty >= qty_missing: # current open order qty is big enough to free-up stocks for selling
                            new_qty = order_qty - qty_missing
                            self.trading_client.replace_order(order.id, qty=new_qty) # Replace the order with reduced quantity
                            time.sleep(1)
                            qty_missing -= qty_missing
                            break
                        else: # current open order qty is NOT enough to free-up stocks for selling
                            self.trading_client.cancel_order_by_id(order_id=order.id) # cancel the order to free up the quantity
                            time.sleep(1)
                            qty_missing -= order_qty
                    # now the qty_available should be sufficient
                self.submit_market_order(ticker = ticker, quantity = qty_to_sell, side = OrderSide.SELL, orderclass = 'simple')
            else:
                msg = f"Skipping {ticker}: not in existing position."
                logger.warning(msg)
                continue
        
        self.sent_alpaca_email(f'{self.strategy_name}',f'{self.strategy_name}: done submitting sell orders for {sell_dictionary}')
        return "sold"


    def _orders_to_dictionary(self, orders_data):
        '''converting strange type of orders to normal dictionary'''
        orders_dicts = []
        for entry in orders_data:
            entry_dict = {}  # Create a new dictionary for each entry
            for attribute, value in vars(entry).items():
                entry_dict[attribute] = value  # Populate the dictionary
            orders_dicts.append(entry_dict)  # Append the populated dictionary to the list
        return orders_dicts


    def _get_orders_info(self, tickers: List[str]=None):
        ''' getting info about filled orders'''

        # searching for the datetime of openning the current positions 
        date_filter = (pd.Timestamp.now()- pd.Timedelta(self.max_holding_time, "days")) # if it a stock in position, it has been bought with max_holding_time
        request_params = GetOrdersRequest(status=QueryOrderStatus.CLOSED, after = date_filter, side=OrderSide.BUY, symbols = tickers)
        orders_dicts = self._orders_to_dictionary(self.trading_client.get_orders(filter=request_params))

        # now we have a problem, that during last 40 days there could be several buy orders for the same tickers (bought => sold => bought),
        # ... so we need just the most recent buying
        recent_orders = {}
        for order in orders_dicts:
            symbol = order['symbol']
            filled_at = order['filled_at'].astimezone(dt.timezone(dt.timedelta(hours=-4))) # converting to ET
            filled_avg_price = round(float(order['filled_avg_price']), 2)
            filled_qty = int(order['filled_qty'])
            strategy = order['client_order_id'].split('_')[0] # deriving strategy from order_id
            if symbol not in recent_orders or filled_at > recent_orders[symbol]['filled_at']:
                recent_orders[symbol] = {'filled_at': filled_at, 'filled_avg_price': filled_avg_price, 'filled_qty': filled_qty, 'strategy': strategy}
        buy_orders_dict = {symbol: [details['filled_at'], details['filled_avg_price'], details['filled_qty'], details['strategy']] for symbol, details in recent_orders.items()}
        return buy_orders_dict


    def trailing_stop_losses(self):
        clock = self.trading_client.get_clock()
        if clock.is_open:
            try:
                # defining currently owned tickers
                    # as I don't buy additionally if I already have stock in pf, we could search for the datetime when position was opened
                positions = self.trading_client.get_all_positions()
                current_positions_symbols = [p.symbol for p in positions]
                logger.info(f'Currently there are {len(current_positions_symbols)} open positions. Start checking drawdown...')

                buy_orders_dict = self._get_orders_info(current_positions_symbols)

                # TODO: we need to compare current_positions_symbols with symbols in buy_orders_dict

                positions_dict_loss = {}
                # for every symbol in positions, check current drawdown and sell if critical
                # Define a function to calculate drawdown
                def calculate_drawdown(prices):
                    max_prices = prices.cummax()
                    drawdown = (prices - max_prices) / max_prices
                    return drawdown

                for symbol, details in buy_orders_dict.items():
                    filled_at = details[0]
                    data_df = self.get_history_minute_single(symbol, filled_at)
                    # Apply the drawdown calculation
                    data_df['drawdown'] = data_df['close'].transform(calculate_drawdown)
                    current_drawdown = data_df.iloc[-1]['drawdown'] # the rows are sorted from oldest to newest, so we take last row
                    logger.info(f'Current drawdown for {symbol} is {current_drawdown:.2%}')

                    # TODO: we compare with simple stop-loss, but ideally it should be low-pass like in zorro
                    if current_drawdown < -self.SimpleStopLoss:
                        positions_dict_loss[symbol] = int(self.trading_client.get_open_position(symbol).qty)

                if positions_dict_loss:
                    logger.info(f'Need to sell these loss positions: {positions_dict_loss}')
                    self.close_selected_positions(positions_dict_loss)

                # usually all position are opened with TP limit order, but just in case we additionally a mnaual TP check
                positions_dict_win = {position.symbol: int(position.qty) for position in positions if float(position.unrealized_plpc) > self.SimpleTakeProfit}
                if positions_dict_win:
                    logger.info(f'Taking profit on these positions: {positions_dict_win}')
                    self.close_selected_positions(positions_dict_win)
            except Exception as e:
                self.sent_alpaca_email('Error in checking drawdown: ',e) # sms + email
                return "error"
        else:
            logger.info(f'Market is closed. No drawdown check needed.')
            return 'done'


    def get_current_positions_df(self):
        positions = self.trading_client.get_all_positions()
        position_dicts = []
        for entry in positions:
            entry_dict = {}  # Create a new dictionary for each entry
            for attribute, value in vars(entry).items():
                entry_dict[attribute] = value  # Populate the dictionary
            position_dicts.append(entry_dict)  # Append the populated dictionary to the list
        current_positions = pd.DataFrame(position_dicts)

        current_positions_symbols = [p.symbol for p in positions]
        buy_orders_dict = self._get_orders_info(current_positions_symbols)
        buy_orders_df = pd.DataFrame.from_dict(buy_orders_dict, orient='index', columns=['filled_at', 'filled_avg_price', 'filled_qty', 'strat'])
        buy_orders_df.reset_index(inplace=True)
        buy_orders_df.rename(columns={'index': 'symbol'}, inplace=True)

        cols_needed = ['symbol','market_value','unrealized_plpc']
        current_positions = current_positions[cols_needed].copy()

        current_positions = current_positions.merge(buy_orders_df, on='symbol', how='left')
        current_positions['filled_at'] = current_positions['filled_at'].dt.tz_convert('Europe/Berlin').dt.tz_localize(None)
        current_date = dt.datetime.now()
        current_positions['days'] = (current_date - current_positions['filled_at']).dt.days

        for column in ['market_value','unrealized_plpc']:
            current_positions[column] = pd.to_numeric(current_positions[column], errors='coerce')
        current_positions['value'] = round(current_positions['market_value'] / 1000, 1)
        current_positions['pl%'] = (current_positions['unrealized_plpc'] * 100).astype(int)
        current_positions = current_positions.sort_values(by='pl%',ascending=False)

        current_positions.drop(columns=['filled_at','unrealized_plpc','market_value', 'filled_avg_price', 'filled_qty'], inplace=True)

        return current_positions.reset_index(drop=True)


    def get_day_summary(self, hoursago = 24):   
        try:
            if hoursago <= 0:
                logger.error(f"Number of hours should be positive.")
                return "Error"

            msg_orders_executed = f"\nNo orders executed for the period."
            msg_orders_open = f"\nNo open orders."
            html_executed = ""
            html_open = ""

            account = self.trading_client.get_account()
            # Check our current balance vs. our balance at the last market close
            balance_change = round(float(account.equity) - float(account.last_equity),0)
            date_filter = (pd.Timestamp.now()- pd.Timedelta(hoursago, "hours")).floor(freq='min') # orders for today # orders for today
            orders = self.trading_client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.CLOSED,after = date_filter))
            if orders:
                closed_symbols = {order.symbol: float(order.filled_avg_price) for order in orders}
                closed_symbols_dict = self._get_orders_info(list(closed_symbols.keys()))
                closed_orders_df = pd.DataFrame.from_dict(closed_symbols_dict, orient='index', columns=['filled_at', 'filled_avg_price', 'filled_qty', 'strat'])
                closed_orders_df.reset_index(inplace=True)
                closed_orders_df.rename(columns={'index': 'symbol'}, inplace=True)
                closed_orders_df['sold_for'] = closed_orders_df.symbol.map(closed_symbols)
                closed_orders_df['pl%'] = round(100*((closed_orders_df['sold_for'] - closed_orders_df['filled_avg_price']) * closed_orders_df['filled_qty'])/(closed_orders_df['filled_avg_price'] * closed_orders_df['filled_qty']),1)
                closed_orders_df['filled_at'] = closed_orders_df['filled_at'].dt.tz_convert('Europe/Berlin').dt.tz_localize(None)
                current_date = dt.datetime.now()
                closed_orders_df['days'] = (current_date - closed_orders_df['filled_at']).dt.days
                closed_orders_df.drop(columns=['filled_at','sold_for','filled_avg_price', 'filled_qty'], inplace=True)
                html_executed = """\
                    <html>
                    <head></head>
                    <body>
                        {0}
                    </body>
                    </html>
                    """.format(closed_orders_df.to_html(index=False))

                msg_orders_executed = (
                        f"\n{len(orders)} orders were executed:\n"
                    )

            open_orders = self.trading_client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN))
            if open_orders:
                df_open = pd.DataFrame(self._orders_to_dictionary(open_orders))
                df_open = df_open[['client_order_id','created_at','symbol','qty','limit_price','order_type','side']].copy()
                df_open['created_at'] = df_open['created_at'].dt.tz_localize(None)

                html_open = """\
                    <html>
                    <head></head>
                    <body>
                        {0}
                    </body>
                    </html>
                    """.format(df_open.to_html(index=False))

                msg_orders_open = (
                        f"\nThere are {len(open_orders)} open orders:\n"
                    )


            df_positions = self.get_current_positions_df()
            html_positions = """\
                <html>
                <head></head>
                <body>
                    {0}
                </body>
                </html>
                """.format(df_positions.to_html(index=False))

            msg = (
                    f"Summary for last {hoursago} hours ending today on {pd.Timestamp.today().day_name()}, {pd.Timestamp.today().date().strftime('%d.%b')}:\n\n"
                    f"\nBalance change: {balance_change}\n\n"
                    f"\nCurrent positions:\n"
                )
            
            html_content = f"{msg}{html_positions}{msg_orders_executed}{html_executed}{msg_orders_open}{html_open}"

            self.sent_alpaca_email('Daily summary', html_content)
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
            message.attach(MIMEText(mail_content, 'html')) # body and the attachments for the mail

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


