# place bracket orders about 9:20 and fetch the latest_bar ....
#   ... and use the close of that bar as my ‘guess’ for what the order will fill at. 
#       +/-10% for “initial” stop loss and take profit 
# when the parent order fills and I have the actual fill price, ...
# ... I replace the stop and limit prices with prices based upon that actual fill. 

# Trailing stop orders are currently only supported for single orders and not part of a bracket or OCO pair

market_order_data = MarketOrderRequest(
    symbol=stock_symbol,
    qty=quantity_to_purchase,
    side=OrderSide.BUY,
    time_in_force=TimeInForce.DAY,
    order_class=OrderClass.BRACKET,
    take_profit=TakeProfitRequest(limit_price=round(float(1.11 * price_per_share), 2)),
    stop_loss=StopLossRequest(stop_price=round(float(0.89 * price_per_share), 2))
    )

market_order = trading_client.submit_order(market_order_data)

order_data = LimitOrderRequest(
    symbol = "AAPL",
    qty = 1,
    limit_price = 125,
    side = OrderSide.BUY,
    type = OrderType.LIMIT,
    time_in_force = TimeInForce.DAY,
    order_class = OrderClass.BRACKET,
    take_profit = TakeProfitRequest(limit_price = 150),
    stop_loss = StopLossRequest(stop_price = 100)
    )

trading_client.submit_order(order_data)

take_profit_leg = my_bracket_order.legs[0]
stop_loss_leg = my_bracket_order.legs[1]
take_profit_order_id = take_profit_leg.id
stop_loss_stop_price = stop_loss_leg.stop_price


# every time an order is replaced, a new order is actually created ...
# ... (ie it’s not really simply updated). 
# The original parent order always maintains the order IDs and info about the most current updates. 
#   Leg[0] is always the take profit order and 
#   Leg[1] the stop loss. 

def update_limit_and_stop_prices():
  today = api.get_clock().timestamp.normalize()
  positions = get_positions_df()

  if not positions.empty:
    orders = get_open_orders_after_df(today)

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



def replace_order(order_id, limit_price=None, stop_price=None):
  '''
  Wrapper for api
  ''' 
  try:
    api.replace_order(order_id, limit_price=limit_price, stop_price=stop_price)

  except Exception as err:
    if err.args[0] == "order parameters are not changed":
      # not a real error
      pass
    else:
      log.error('tried to replace order {} limit:{} stop:{} {}'.format(order_id, limit_price, stop_price, err))

  return

