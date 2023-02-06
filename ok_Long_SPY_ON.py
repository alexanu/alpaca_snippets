from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest

import logging

# trading setup
strategy_name = "Long_Overnight"
strategy_weight = 0.2 # 20% from all buying power of my account will be dedicated to this strategy

from Alpaca_config import *
trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER)
stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


def buy_close(_symbol, _qty, _coid):
    try:
        market_order_data = MarketOrderRequest(
            symbol=_symbol,
            qty=_qty, # any unfilled orders after the open will be cancelled
            side=OrderSide.BUY,
            client_order_id = _coid,
            time_in_force=TimeInForce.CLS) # “market on close (MOC) 
        print(f"Buying {_qty} of {_symbol} on close auction")
        return trading_client.submit_order(order_data=market_order_data)
    except Exception as _e:
        logging.error(_e)
        return None

def sell_open(_symbol, _qty, _coid):
    try:
        market_order_data = MarketOrderRequest(
            symbol=_symbol,
            qty=_qty, # any unfilled orders after the open will be cancelled
            side=OrderSide.SELL,
            client_order_id = _coid,
            time_in_force=TimeInForce.OPG) # “market on open” (MOO) 
        print(f"Selling {_qty} of {_symbol} on open auction")
        return trading_client.submit_order(order_data=market_order_data)
    except Exception as _e:
        logging.error(_e)
        return None


def main(_symbol):
    print(f'Today is {pd.Timestamp.today().day_name()}, {pd.Timestamp.now(tz="CET").tz_localize(None).strftime("%b %d, %H:%M") } in Munich, which is {pd.Timestamp.now(tz="EST").tz_localize(None).strftime("%H:%M")} in New York')
    clock = trading_client.get_clock()

    # if market not open, exit
    if not clock.is_open:
        time_to_open = (clock.next_open - clock.timestamp).total_seconds()//3600
        print(f'Market is currently closed. Will open in {time_to_open} hours')
        exit()
    else:
        time_to_close = (clock.next_close - clock.timestamp).total_seconds()//60
        positions = trading_client.get_all_positions()
        positions_symbols_set = {p.symbol for p in positions}
        if positions:
            [print(f"{p.symbol} with profit of {p.unrealized_pl}",end="; ") for p in positions]
        else:
            print("No opened positions")

        # validate if SELL is happening near market open time and if BUY is happening near market close time
        latest_ask_price = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[_symbol]))[_symbol].ask_price
        if latest_ask_price:
            # if it's close to market close time, then BUY (only if we don't have any opened positions)
            if (clock.timestamp.strftime('%H:%M') > '9:35' or clock.timestamp.strftime('%H:%M') < '15:49') and _symbol not in positions_symbols_set:
            # Market-on-close orders submitted after 3:50pm but before 7:00pm ET will be rejected
                quantity_to_buy_at_close = int(strategy_weight * float(trading_client.get_account().buying_power)//latest_ask_price)
                coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
                buy_close(_symbol,quantity_to_buy_at_close,coid)
                print(f"New buing power is {float(trading_client.get_account().buying_power)}")
            if (clock.timestamp.strftime('%H:%M') > '19:00' or clock.timestamp.strftime('%H:%M') < '9:20') and _symbol in positions_symbols_set:
            # Market-on-open orders submitted after 7:00pm will be queued and routed to the following day’s opening auction
                coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
                sell_open(_symbol,quantity,coid)
                print(f"New buing power is {float(trading_client.get_account().buying_power)}")
        else:
            logging.warning(f"Cannot get the last 15-minute delayed price for {_symbol}!")


if __name__ == "__main__":
    main(SYMBOL)