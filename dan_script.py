# First install and import the Alpaca python API SDK 'wrapper'
import alpaca_trade_api as alpacaapi

# Import some useful packages
import exchange_calendars
import numpy as np
import pandas as pd

### Set which account to trade ###
ENVIRONMENT = 'LIVE' # PAPER or LIVE

if ENVIRONMENT == 'PAPER':
  # Alpaca keys etc
  ALPACA_API_ENDPOINT = 'https://paper-api.alpaca.markets'

  # Below is test paper account 
  ALPACA_API_KEY = 'xxxxx'
  ALPACA_API_SECRET_KEY = 'xxxxxx'

elif ENVIRONMENT == 'LIVE':
  # Alpaca keys etc
  ALPACA_API_ENDPOINT = 'https://api.alpaca.markets'

  # Below is live account 
  ALPACA_API_KEY = 'xxxxxx'
  ALPACA_API_SECRET_KEY = 'xxxxx'

api = alpacaapi.REST(ALPACA_API_KEY, ALPACA_API_SECRET_KEY, ALPACA_API_ENDPOINT)
api_data = alpacaapi.REST(ALPACA_API_KEY, ALPACA_API_SECRET_KEY, ALPACA_API_ENDPOINT)

# instantiate a calendar object
market = exchange_calendars.get_calendar("XNYS")

### Set global parameters ###
# Set constants
LEVERAGE = 1.0

# stock qty and weights
LONG_QTY = 50
SHORT_QTY = 50

LONG_WEIGHT = .5
SHORT_WEIGHT = .5

# tradable universe rules
MIN_DOLLAR_VOL = 10000000
MIN_PRICE = 3
MAX_PRICE = 1000

# order constants
INITIAL_TAKE_PROFIT_PCT = .2
TAKE_PROFIT_PCT = .10
STOP_LOSS_PCT = .01
MAKE_MARKETABLE_PCT = .01


def handle_request(request):
  """
  Entry Point into function
  Selects local function run based on query parameter
  """
  function = request.args.get('function')

  current_time = pd.Timestamp.now(market.tz)

  # is today a trading day
  today_is_a_trading_day = market.is_session(current_time.date())

  # is today an early close 
  today_is_an_early_close = current_time.date() in market.early_closes.date

  if today_is_a_trading_day and not today_is_an_early_close:

    if function == 'open_intraday':
        open_intraday()

    elif function == 'update_limit_prices':
        update_limit_prices()

    elif function == 'replace_orders_to_fill':
      replace_orders_to_fill()

    elif function == 'close_positions':
      close_positions()
 
    else:
      # Invalid HTTP query parameter
      
      print('❗️ invalid HTTP request {}'.format(request))
  return "success"

##### Called Functions #####

# is called once before markets open and long and short OTO orders are placed based upon the trade rules
def open_intraday():
# get the current and previous trade dates
  current_time = pd.Timestamp.now(market.tz)
  current_trading_day = market.minute_to_session(current_time).tz_localize(market.tz)
  prev_trading_day = market.minute_to_past_session(current_time).tz_localize(market.tz)

  # get tradable universe 
  tradable_symbols = get_tradable_symbols()

  # need to fix why potential_wash_sale_symbols got error on Jan 31
  #potential_wash_sale_symbols = get_potential_wash_sale_symbols(current_trading_day)
  #tradable_symbols = list(set(tradable_symbols) - set(potential_wash_sale_symbols))
  
  # get data
  prev_day_bars = get_1_day_daily_bars(prev_trading_day, tradable_symbols)

  # assemble data needed to trade
  trade_data = create_trade_bars(prev_day_bars)

  # find todays trades
  # filters for positive close to open gain. 
  GAIN =  ' \
        dollar_volume > @MIN_DOLLAR_VOL \
        and @MIN_PRICE < close < @MAX_PRICE \
        and close_open_gain >= 0  \
        and close_open_gain_zscore > 0  \
        '

  # doesn't trade if market is way off lows (ie mean close to low is high)
  GAIN_PLUS =  ' \
          dollar_volume > @MIN_DOLLAR_VOL \
          and @MIN_PRICE < close < @MAX_PRICE \
          and close_open_gain >= 0  \
          and close_low_ratio_mean < .0175 \
          '

  long_closing_near_high = (trade_data.query(GAIN).nlargest(LONG_QTY, 'close_high_ratio_zscore').index)

  short_closing_near_high = (trade_data.query(GAIN_PLUS + ' and index not in @long_closing_near_high').
                            nsmallest(SHORT_QTY // 2, 'close_high_ratio_zscore').index)
  short_closing_off_low = (trade_data.query(GAIN_PLUS + ' and index not in @long_closing_near_high and index not in @short_closing_near_high').
                          nlargest(SHORT_QTY // 2, 'close_low_ratio_zscore').index)

  # place orders
  long_orders = create_orders(long_closing_near_high, 'buy', LONG_WEIGHT, LONG_QTY)
  short_orders = create_orders(short_closing_near_high.union(short_closing_off_low), 'sell', SHORT_WEIGHT, SHORT_QTY)

  all_orders = pd.concat([long_orders, short_orders])

  place_oto_orders(all_orders, INITIAL_TAKE_PROFIT_PCT, LEVERAGE)
  return


# is called every minute during market hours to close out positions in sort of a stop loss fashion
def update_limit_prices():

  # get current positions and orders
  positions = get_positions_df()

  if not positions.empty:
    orders = get_open_limit_orders()

    if not orders.empty:
      # there are positions and open limit orders (we assume they match)
      # add columns for values we will need
      orders['current_price'] = positions.current_price.astype('float')
      orders['entry_price'] = positions.avg_entry_price.astype('float')
      
      orders['target_sell_limit_price'] = np.around(orders.entry_price * (1 + TAKE_PROFIT_PCT), decimals=2)
      orders['target_buy_limit_price'] = np.around(orders.entry_price * (1 - TAKE_PROFIT_PCT), decimals=2)
      
      orders['sell_stop_price'] = np.around(orders.entry_price * (1 - STOP_LOSS_PCT), decimals=2)
      orders['buy_stop_price'] = np.around(orders.entry_price * (1 + STOP_LOSS_PCT), decimals=2)
      
      # get todays minute bars
      minute_bars = get_todays_market_bars(orders.index).tz_convert(market.tz)
      
      if not minute_bars.empty:
        from pykalman import KalmanFilter
        for symbol, order in orders.iterrows():
          # calculate Kalman vwap
          # Instantiate a Kalman filter and use it to smooth the minute data
          kf = KalmanFilter(transition_matrices = [1],
                            observation_matrices = [1],
                            initial_state_mean = order.entry_price,
                            initial_state_covariance = 1,
                            observation_covariance=1,
                            transition_covariance=.01)

          smoothed_vwap, _ = kf.filter(minute_bars.query('symbol == @symbol').vwap)
          
          if order.side=='sell':
            if smoothed_vwap.min() < order.sell_stop_price:
              # bought long but now price is below stop price
              # try to close position
              target_limit_price = np.around(order.current_price, decimals=2)
            else:
              # set limit to the take profit price
              target_limit_price = order.target_sell_limit_price
          
          elif order.side=='buy':
            if smoothed_vwap.max() > order.buy_stop_price:
              # sold short but now price is above stop price
              # try to close position
              target_limit_price = np.around(order.current_price, decimals=2)
            else:
              # set limit to the take profit price
              target_limit_price = order.target_buy_limit_price
            
          if order.limit_price != target_limit_price:
            replace_order(order.id, limit_price=target_limit_price)
            
  return

# runs before markets close to close all open positions by lowering the limit prices to a marketable value
def replace_orders_to_fill():
  """
  replace all open buy limit orders with a marketable price
  with the intention that order will fill
  """
  # get current positions and orders
  positions = get_positions_df()

  if not positions.empty:
    orders = get_open_limit_orders()

    if not orders.empty:
      # there are positions and open limit orders (we assume they match)
      # add columns for values we will need
      orders['bid'], orders['ask'] = get_latest_quotes(orders.index)
      orders['marketable_sell_price'] = np.around(orders.bid * (1 - MAKE_MARKETABLE_PCT), decimals=2)
      orders['marketable_buy_price'] = np.around(orders.ask * (1 + MAKE_MARKETABLE_PCT), decimals=2)

      for order in orders.itertuples():
        marketable_price = order.marketable_sell_price if order.side=='sell' else order.marketable_buy_price
        if order.limit_price != marketable_price:
          replace_order(order.id, limit_price=marketable_price)

  return

# runs once just before markets close in case there are any open positions (just a failsafe)
def close_positions():
  """
  Close intra day
  The SDK version doesn't have the cancel_orders option.
  """
  positions = get_positions_df()

  if not positions.empty:
    # log the fact that not all postions closed
    print('❗️ some positions didnt close with limit. Deleting positions')
    api.delete('/positions', data='cancel_orders=True')

  return