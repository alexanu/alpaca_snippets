from utils_for_alpaca import get_open_orders_after_df, get_positions_df

# first set some 'global' constants
TAKE_PROFIT_PCT = .1
STOP_LOSS_PCT = .03

def replace_order(order_id, limit_price=None, stop_price=None):
    try:
        api.replace_order(order_id, limit_price=limit_price, stop_price=stop_price)
    except Exception as err:
        log.error('tried to replace order {} limit:{} stop:{} {}'.format(order_id, limit_price, stop_price, err))
    return

# run this function after all the initial parent orders fill
def update_limit_and_stop_prices():
  """
  Update any open limit and stop orders for current positions.
  Updated prices based on constants TAKE_PROFIT_PCT and STOP_LOSS_PCT.
  This only updates orders placed today (ie the day it is run)
  """
  # get current positions and orders
  today = api.get_clock().timestamp.normalize()
  orders = get_open_orders_after_df(today)
  positions = get_positions_df()

  # add a column for entry price on each order
  orders['entry_price'] = positions.avg_entry_price.astype('float')

  # determine the types of orders
  limit_sell_orders = orders.query('type=="limit" and side=="sell"')
  limit_buy_orders = orders.query('type=="limit" and side=="buy"')
  stop_sell_orders = orders.query('type=="stop" and side=="sell"')
  stop_buy_orders = orders.query('type=="stop" and side=="buy"')

  # update the orders with new limit or stop price
  for order in limit_sell_orders.itertuples():
    replace_order(order.id, limit_price=round((1+TAKE_PROFIT_PCT)*order.entry_price, 2))

  for order in limit_buy_orders.itertuples():
    replace_order(order.id, limit_price=round((1-TAKE_PROFIT_PCT)*order.entry_price, 2))

  for order in stop_sell_orders.itertuples():
    replace_order(order.id, stop_price=round((1-STOP_LOSS_PCT)*order.entry_price, 2))

  for order in stop_buy_orders.itertuples():
    replace_order(order.id, stop_price=round((1+STOP_LOSS_PCT)*order.entry_price, 2))