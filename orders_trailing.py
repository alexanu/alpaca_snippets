import alpaca_trade_api as tradeapi
import pandas as pd
from src import config

api = tradeapi.REST(config.ALP_API_KEY, config.ALP_SECRET_KEY, base_url=config.ALP_API_URL)

def get_positions():
    positions_list = api.list_positions()
    positions = []
    for position in positions_list:
        symbol = position.symbol
        qty = position.qty
        positions.append((symbol, qty))
    return positions


def cancel_orders_for_positions(symbol):
    orders = api.list_orders(status='open')
    for order in orders:
        if order.symbol in symbol and order.type != 'trailing_stop':
            api.cancel_order(order.id)
            print(f"Cancelling order {order.symbol} with ID {order.id}")
    return symbol

def place_trailing_stop_order(symbol, qty, trail_percent):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side='sell',  # or 'buy' depending on the position
            type='trailing_stop',
            time_in_force='gtc',
            trail_percent=trail_percent
        )
        return order

    except Exception as err:
        return None

###########################


def main():
    trail_percent = str(1.0)

    positions = get_positions()

    for symbol, qty in positions:
        cancel_orders_for_positions(symbol)

    for symbol, qty in positions:
        place_trailing_stop_order(symbol, qty, trail_percent)


if __name__ == "__main__":
    main()