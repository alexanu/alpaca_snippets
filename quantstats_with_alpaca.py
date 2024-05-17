# First install and import the Alpaca python API SDK 'wrapper'
#import logging as log
import alpaca_trade_api as alpacaapi
import quantstats as qs # pip install quantstats

api = alpacaapi.REST(ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY, ALPACA_API_ENDPOINT)

history = api.get_portfolio_history(date_start='2023-01-01', date_end='2023-05-01')
dates = pd.to_datetime(history.timestamp, unit='s')
equities = history.equity
equity_df = pd.DataFrame(data=equities, index=dates, columns=['equity'])
equity_df['daily_returns'] = equity_df.pct_change()

qs.reports.full(equity_df.daily_returns)

sharpe = qs.stats.sharpe(equity_df.daily_return)
max_drawdown = qs.stats.max_drawdown(equity_df.daily_return)