# Alpaca for data
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame


alpaca_stream = Stream(API_KEY, API_SECRET) # instance of data streaming API

async def on_crypto_bar(bar): # handler for receiving bar data for Bitcoin
    if bar.exchange != 'CBSE':
        return
    print(bar)
alpaca_stream.subscribe_crypto_bars(on_crypto_bar, "BTCUSD")

async def on_equity_bar(bar): # handler for receiving bar data for Coinbase stock
    print(bar)
alpaca_stream.subscribe_bars(on_equity_bar, "COIN")

alpaca_stream.run()

async def on_equity_bar(bar):
   synch_datafeed(bar)    # bar data passed to intermediary function
 
async def on_crypto_bar(bar):
   if bar.exchange != 'CBSE':
       return
   synch_datafeed(bar)    # bar data passed to intermediary function

data = {}
 
def synch_datafeed(bar):
    # convert bar timestamp to human readable form
    time = datetime.fromtimestamp(bar.timestamp / 1000000000)
    symbol = bar.symbol

    if time not in data:    # If we’ve never seen this timestamp before
        data[time] = {symbol:bar}        # Store bar data in a dictionary keyed by symbol
        return

    data[time][symbol] = bar
    timeslice = data[time]
    on_synch_data(timeslice)

def on_synch_data(data):
    btc_data = data["BTCUSD"]
    coin_data = data["COIN"]
    btc_close = btc_data.close
    coin_close = coin_data.close
    spread = btc_close - coin_close
    spread_std = historical_spread_std
    # calculate entry and exit levels for standard deviation
    entry_level = -1 * spread_std
    loss_exit_level = -3 * spread_std

    # pass spread and level data to next part for placing trades
    place_trades(spread, enty_level, loss_exit_level)

def place_trades(spread, entry_level, loss_exit_level):
     
	# there is an active position if there is at least 1 position
      active_position = len(alpaca_trade.list_positions()) != 0    
 
if spread < entry_level and not active_position:
     # retrieve buying power from account details
     buying_power = alpaca_trade.get_account().buying_power
           # the buying power allocated to each asset will be half of the total
	     btc_notional_size = buying_power // 2
           coin_notional_size = buying_power // 2
           
          # place long order on BTCUSD
           alpaca_trade.submit_order(symbol="BTCUSD", notional=btc_notional_size, type='market', side='buy', time_in_force='day')
 
          # Place short order for COIN
           alpaca_trade.submit_order(symbol="COIN", notional=coin_notional_size, type='market', side='sell', time_in_force='day')
 
   elif spread < loss_exit_level and active_position:
       # liquidate if loss exit level is breached
       alpaca_trade.close_all_positions()
 
   elif spread > 0 and active_position:
       # liquidate if 0 spread is crossed with an active position
       alpaca_trade.close_all_positions()