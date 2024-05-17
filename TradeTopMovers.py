
from utils_for_alpaca import MyAlpaca, logger

import os
import pandas as pd
from github import Github
import io

from Alpaca_config import *


import os

# List all environment variables
for key, value in os.environ.items():
    print(f'{key}: {value}')

API_KEY_PAPER = os.environ['API_KEY_PAPER']
API_SECRET_PAPER = os.environ['API_SECRET_PAPER']
alpaca_instance = MyAlpaca(API_KEY_PAPER,API_SECRET_PAPER)


alpaca_instance.get_day_summary()

support_trade_instance = SupportTrade()
support_trade_instance.sent_alpaca_email('AAAA','BBBB')
market = alpaca_instance.check_trading_day()

repository = Github(github_strat_token).get_user().get_repo(dedicated_repo)
strategies = repository.get_contents(gh_xls_strategy)
trading_ideas_df = pd.read_excel(io.StringIO(strategies.decoded_content.decode()), sep = ";")


# Example usage
# alpaca_instance.submit_bracket_order(...)
# support_trade_instance.inform("Your message")
# logger.info("This is an info message from strategy_script")
logger.error("This is an error message, will be sent via email")

alpaca_instance.check_trading_day()

strategy_config ='strategy_config'
strategy_family = 'Orders_at_23CET_(post_top)'

max_investment_per_position = 2000
num_to_buy = 1
strategy_weight = 0.2 # 20% from all buying power of my account will be dedicated to this strategy
max_items = 10 # I don't want to have more than 10 stocks in pf
invoked_via = "SDK"
strategy_name = strategy_family+"_"+str(max_investment_per_position)+"_"+str(max_items)+invoked_via
investment = min(max_investment_per_position,(strategy_weight * float(trading_client.get_account().buying_power)/max_items)) # either max allowed either rest of cash

positions = alpaca_instance.trading_client.get_all_positions()
positions_dict = {position.symbol: position.market_value for position in positions}
scope = alpaca_instance.get_ok_alpaca_stocks(spread_limit=0.005)
data_df = alpaca_instance.get_tkrs_snapshot_df(scope)
ticker_to_buy_prelim = data_df.sort_values(by='POST', ascending=False)['symbol'].head(3).tolist()
[alpaca_instance.submit_market_order(ticker = stock, money = max_investment_per_position, strategy_name=strategy_name) for stock in ticker_to_buy_prelim]

data_intraday = alpaca_instance.get_history(symbols=scope,periods=500)
data_daily = alpaca_instance.get_history(symbols=scope,periods=500,FrameLength=1,frame='day')
new_df = alpaca_instance.add_columns(data_intraday)

latest_timestamp = data_intraday['timestamp'].max()
latest_df = data_intraday[data_intraday['timestamp'] == latest_timestamp].copy()
latest_df['ema-close'] = (latest_df['close'] - latest_df['ema']) / latest_df['ema']
latest_df = latest_df.sort_values(by='ema-close')
ticker_to_buy_prelim = latest_df.sort_values(by='ema-close')['symbol'].head(3).tolist() # buying stocks with largest gap to EMA hoping for reversing
ticker_to_buy_prelim = latest_df.sort_values(by='streak',ascending = False)['symbol'].head(3).tolist() # buying stocks with largest gap to EMA hoping for reversing

to_buy = set(latest_df.head()['symbol'].to_list())
to_sell = set(latest_df.tail()['symbol'].to_list())





df = data_intraday.copy()
df[['symbol','timestamp','close', 'logret']]
df[df['timestamp']=='2023-12-08 09:30:00']
df[df['timestamp']=='2023-12-22 16:00:00']

