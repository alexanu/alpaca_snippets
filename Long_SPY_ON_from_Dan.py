# https://pdfs.semanticscholar.org/702c/38db68f547d25ab85be1ce7c2f66edffff7e.pdf

# Long SPY overnight then short intraday + stay out in times of high volatility and extreme market jumps. 

# Run in the cloud

# the only fee incurred with this strategy is margin for the ETF held overnight. 
# There of course isn’t any commission, and I intentionally short only intraday. 
# If one doesn’t hold anything short overnight then there aren’t any associated borrow fees
#  (borrow fees are only assessed on overnight holdings). 


# The above numbers do not take into account dividends. 
# Another reason I intentionally do not short overnight is dividends. 
# One must pay (not receive) dividends for any stock held overnight on the dividend payment date. 
# Since this strategy only shorts intraday there aren’t any dividend liabilities. 
# There are however credits for the long stock held overnight if held on a dividend pay date. 
# The dividend payout in the overnight ETF is about 1.5%. 
# Therefore the annual dividend is about .015 x 1.25 or about 1.8%. 
# When live trading this I found dividends accounted for a little less (closer to 1.2%) since every so often I would miss a payment date. 


# First install and import the Alpaca python API SDK 'wrapper'
import logging as log

import alpaca_trade_api as alpacaapi
from alpaca_trade_api.rest import TimeFrame

# Import some useful packages
import math
import numpy as np
import pandas as pd

# Set constants

    # Intraday I actually short DIA (not SPY): Alpaca doesn't allow moving a position from long to short (or vice versa) in a single step. 
    # Moving between two similar ETFs is a workaround. DIA seemed like a close proxy. 
ETF_TO_HOLD_INTRA_DAY = 'SPY' # SP500 ETF
ETF_TO_HOLD_OVERNIGHT = 'DIA' # DOW ETF
SPY = 'SPY'

INTRADAY_LEVERAGE = 1.25
OVERNIGHT_LEVERAGE = 1.25

MAX_INTRA_DAY_LOSS = -.009
MAX_INTRA_DAY_GAIN = .009

MAX_OVERNIGHT_LOSS = 0.0
MAX_OVERNIGHT_GAIN = .004

MAX_VOLATILITY = .015

PREV_STD_DAYS = 4

BUYING_POWER_CUSHION = 1.0 - .01


# It's run as a single cloud function deployed on Google Cloud Functions, and triggered from 4 Google Scheduler  HTTP requests. 
# The functions are scheduled 
#   just before markets open (open an intraday ETF), 
#   after markets open (close it), 
#   before markets close (open an overnight ETF), 
#   and after markets close (close it). 


def handle_request(request):
  """
  Entry Point into function
  Selects local function run based on query parameter
  """
  function = request.args.get('function')
  log.info('request was {}'.format(function))

  if function == 'open_intraday_holdings':
    open_intraday_holdings()

  elif function == 'close_intraday_holdings':
    close_intraday_holdings()

  elif function == 'open_overnight_holdings':
    open_overnight_holdings()

  elif function == 'close_overnight_holdings':
    close_overnight_holdings()

  else:
    # Invalid HTTP query parameter
    log.error('invalid HTTP request {}'.format(request))

  return

##### Called Functions #####

def open_intraday_holdings():
  ok_overnight_gain = MAX_OVERNIGHT_LOSS < overnight_gain(SPY) < MAX_OVERNIGHT_GAIN # previous overnight SPY gain is a little positive (not too high or negative)
  ok_volatility = daily_intraday_volatilty(SPY) < MAX_VOLATILITY # n-day intra-day volatility for SPY is not too high

  if ok_overnight_gain and ok_volatility:
    # open intraday ETF short for the day
    order_target_percent(ETF_TO_HOLD_INTRA_DAY, -INTRADAY_LEVERAGE, time_in_force='opg', is_day_trade=True) 
                                                                        # opg: to submit “market on open” (MOO) and “limit on open” (LOO) orders - 
                                                                        # executed only in the market opening auction. 
                                                                        # Any unfilled orders after the open will be cancelled. 
                                                                        # OPG orders submitted after 9:28am but before 7:00pm ET will be rejected. 
                                                                        # OPG orders submitted after 7:00pm will be queued and routed to the following day’s opening auction. 
    log.info('Overnight gain and volatility OK. Short intraday ETF')
  else:
    # This shouldn't be open but close if it is.
    order_target_percent(ETF_TO_HOLD_INTRA_DAY, 0, time_in_force='opg', is_day_trade=False)
    log.info('Overnight gain or volatility not OK. Pass on intraday ETF')

  return

def close_intraday_holdings():
  """
  Close intra day ETF at end of day if held.
  Do this early in case it's a short trading day
  """
  order_target_percent(ETF_TO_HOLD_INTRA_DAY, 0, time_in_force='cls', is_day_trade=True)
                                                    # cls: to submit “market on close” (MOC) and “limit on close” (LOC) orders. 
                                                    # This order is eligible to execute only in the market closing auction. 
                                                    # Any unfilled orders after the close will be cancelled. 
                                                    # CLS orders submitted after 3:50pm but before 7:00pm ET will be rejected. 
                                                    # CLS orders submitted after 7:00pm will be queued and routed to the following day’s closing auction.
  return

def open_overnight_holdings():

  ok_intra_day_gain = MAX_INTRA_DAY_LOSS < intraday_gain(SPY) < MAX_INTRA_DAY_GAIN # intra-day SPY gain is a little positive (not too high or negative) 
  ok_volatility = daily_intraday_volatilty(SPY) < MAX_VOLATILITY # n-day intra-day volatility for SPY is not too high
  if ok_intra_day_gain and ok_volatility:
    order_target_percent(ETF_TO_HOLD_OVERNIGHT, OVERNIGHT_LEVERAGE, time_in_force='cls', is_day_trade=False)
                                                                        # cls: to submit “market on close” (MOC) and “limit on close” (LOC) orders. 
                                                                        # This order is eligible to execute only in the market closing auction. 
                                                                        # Any unfilled orders after the close will be cancelled. 
                                                                        # CLS orders submitted after 3:50pm but before 7:00pm ET will be rejected. 
                                                                        # CLS orders submitted after 7:00pm will be queued and routed to the following day’s closing auction.
    log.info('Intraday gain and volatility OK. Long overnight ETF')
  else:
    # This shouldn't be open but close if it is.
    order_target_percent(ETF_TO_HOLD_OVERNIGHT, 0, time_in_force='cls', is_day_trade=False)
    log.info('Intraday gain or volatility not OK. Pass on overnight ETF')
  return

def close_overnight_holdings():
  """
  Close our overnight ETF at open next day.
  """
  order_target_percent(ETF_TO_HOLD_OVERNIGHT, 0, time_in_force='opg', is_day_trade=False)
                                                        # opg: to submit “market on open” (MOO) and “limit on open” (LOO) orders - 
                                                        # executed only in the market opening auction. 
                                                        # Any unfilled orders after the open will be cancelled. 
                                                        # OPG orders submitted after 9:28am but before 7:00pm ET will be rejected. 
                                                        # OPG orders submitted after 7:00pm will be queued and routed to the following day’s opening auction. 
  return



##### Helper Functions #####

def overnight_gain(stock):
  """
  Calculates overnight gain (last close to current price)
  """
  # Get snapshot data
  snapshot_data = api_data.get_snapshot(stock)

  # Check if data is current
  clock = api.get_clock()
  current_time = clock.timestamp
  current_date = clock.timestamp.normalize()

  minute_bar_is_old = snapshot_data.minute_bar.timestamp < current_time - pd.Timedelta(15, 'minutes')
  if minute_bar_is_old:
    log.warning('minute data is more than 15 minutes old. timestamp. is: {}'.format(snapshot_data.minute_bar.timestamp))

  daily_bar_is_old = snapshot_data.daily_bar.timestamp < current_date
  if daily_bar_is_old:
    log.warning('daily data isnt current. timestamp. is: {}'.format(snapshot_data.daily_bar.timestamp))

  # Calculate gain (even if old data)
  price_current = snapshot_data.minute_bar.close
  price_prev_close = snapshot_data.prev_daily_bar.close

  gain_close_to_current = math.log(price_current / price_prev_close)
  log.debug('overnight gain calc. current price: {}  prev close price: {}  gain: {}'.format(price_current, price_prev_close, gain_close_to_current ))

  return gain_close_to_current

def intraday_gain(stock):
  """
  Calculates intra-day gain (open to current price)
  """
  # Get snapshot data
  snapshot_data = api_data.get_snapshot(stock)

  # Check if data is current
  clock = api.get_clock()
  current_time = clock.timestamp
  current_date = clock.timestamp.normalize()

  minute_bar_is_old = snapshot_data.minute_bar.timestamp < current_time - pd.Timedelta(15, 'minutes')
  if minute_bar_is_old:
    log.warning('minute data is more than 15 minutes old. timestamp. is: {}'.format(snapshot_data.minute_bar.timestamp))

  daily_bar_is_old = snapshot_data.daily_bar.timestamp < current_date
  if daily_bar_is_old:
    log.warning('daily data isnt current. timestamp. is: {}'.format(snapshot_data.daily_bar.timestamp))

  # Calculate gain (even if old data)
  price_current = snapshot_data.minute_bar.close
  price_open = snapshot_data.prev_daily_bar.open

  gain_open_to_current = math.log(price_current / price_open)
  log.debug('intraday gain calc. current price: {}  open price: {}  gain: {}'.format(price_current, price_open, gain_open_to_current ))

  return gain_open_to_current

def daily_intraday_volatilty(stock):
  """
  Calculate n day volatility of intra_day gains
  """
  # Fetch more days than needed
  today = api.get_clock().timestamp.date()
  previous_day = today - pd.Timedelta('1D')
  previous_day_10 = today - pd.Timedelta('10D')

  bar_data = api.get_bars(stock, TimeFrame.Day, previous_day_10.isoformat(), previous_day.isoformat(), 'raw' ).df

  bar_data['intra_day_gain'] = np.log(bar_data.close/bar_data.open)
  bar_data['volatility'] = bar_data.rolling(PREV_STD_DAYS).intra_day_gain.std()
  volatility = bar_data.volatility[-1]
  log.debug('intraday volatility calc: {}'.format(volatility))

  return volatility

def order_target_percent(symbol, percent, time_in_force, is_day_trade):
  '''
  Places an order to get to the target percent of portfolio
  Does not do fractional shares
  Doesn't go from long->short or short->long. Will close position and raise an error.
  '''
  # Get the needed position and account data
  positions_list = api.list_positions()
  positions_dict = {position.symbol: position for position in positions_list}

  # Get stock price if not in positions
  if symbol not in positions:
    current_price = api.get_snapshot("SPY").minute_bar.close
    current_dollar_amt = 0.0
    current_qty = 0.0
  else:
    current_price = float(positions[symbol].current_price)
    current_dollar_amt = float(positions[symbol].market_value)
    current_qty = float(positions[symbol].qty)

  account = api.get_account()
  portfolio_value = float(account.equity)
  target_dollar_amt = (portfolio_value * percent)

  # Calc target qty. Always round down and with integer share qty
  target_qty = int(target_dollar_amt / current_price)
  delta_qty = target_qty - current_qty
  delta_amt = delta_qty * current_price

  amt_has_different_sides = (current_dollar_amt<0 and target_dollar_amt>0) or (current_dollar_amt>0 and target_dollar_amt<0)
  decrease = not amt_has_different_sides and (abs(target_qty) < abs(current_qty))
  
  if (target_dollar_amt==0) or amt_has_different_sides: # Simply close the position
    try:
      order = api.submit_order(stock, abs(current_qty), side, 'market', time_in_force)
      log.info('ordered {} shares of {}'.format(qty, symbol))
    except Exception as err:
      log.error('tried to order {} shares of {}. {}'.format(qty, symbol, err))
  
  elif decrease: # order delta shares
    try:
      order = api.submit_order(symbol, abs(delta_qty), side, 'market', time_in_force)
      log.info('ordered {} shares of {}'.format(delta_qty, symbol))
    except Exception as err:
      log.error('tried to order {} shares of {}. {}'.format(delta_qty, symbol, err))


  else: # increase in the position
    buying_power = float(account.daytrading_buying_power) if is_day_trade else float(account.regt_buying_power)
    max_amt = min(buying_power * BUYING_POWER_CUSHION, abs(delta_amt))
    adjusted_amt = math.copysign(max_amt, delta_amt)
    adjusted_qty = int(adjusted_amt / current_price)
    try:
      order = api.submit_order(symbol, abs(adjusted_qty), side, 'market', time_in_force)
      log.info('ordered {} shares of {}'.format(adjusted_qty, symbol))
    except Exception as err:
      log.error('tried to order {} shares of {}. {}'.format(adjusted_qty, symbol, err))

    if adjusted_amt < delta_amt:
      log.warning('not enough buying power. day trade: {}  desired trade amt: {}  buying power {}'.format(is_day_trade, delta_amt, buying_power))

  return order
