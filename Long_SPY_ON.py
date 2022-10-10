from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, TrailingStopOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from dateutil.tz import tzlocal
from datetime import datetime
import dateutil.relativedelta
import logging

# trading setup
SYMBOL = 'SPY'
PAPER = True  # paper trading
TIME_BUFFER = 900  # time buffer to trade after market is open and before market is close, in seconds

from Alpaca_config import *
trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER)
data_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


def list_positions():
    try:
        positions = trading_client.get_all_positions()
        return [position for position in positions if positions]
    except Exception as _e:
        logging.error(_e)
        return None


def buy_close(_symbol):
    try:
        return trading_client.submit_order(
            order_data=TrailingStopOrderRequest(
                symbol=_symbol,
                qty=1,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                trail_percent=2,
                extended_hours=False
            )
        )
    except Exception as _e:
        logging.error(_e)
        return None


def sell_open(_symbol):
    try:
        return trading_client.close_all_positions(cancel_orders=True)
    except Exception as _e:
        logging.error(_e)
        return None


def get_symbol_last_price(_symbol):
    try:
        latest_quote = data_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[_symbol]))
        return latest_quote[_symbol].ask_price
    except Exception as _e:
        logging.error(_e)
        return None


def main():
    logging.info(trading_client.get_account().status)
    clock = trading_client.get_clock()
    open_at = clock.next_open.astimezone(tzlocal())
    close_at = clock.next_close.astimezone(tzlocal())
    time_now = datetime.now(tzlocal())

    # if market not open, exit
    if not clock.is_open:
        remaining_time = dateutil.relativedelta.relativedelta(time_now, open_at)
        logging.info(f"Market not open! {remaining_time}")
        exit()
    else:
        if list_positions():
            logging.info(list_positions())
        else:
            logging.info("No opened positions!")

        # validate if SELL is happening near market open time and if BUY is happening near market close time
        if get_symbol_last_price(SYMBOL):
            # if it's close to market close time, then BUY (only if we don't have any opened positions)
            if round((close_at-time_now).total_seconds()) < TIME_BUFFER and not list_positions():
                logging.info(buy_close(SYMBOL))
            # if it's right after the market open time, then SELL (if we have open positions)
            if round((time_now-open_at).total_seconds()) < TIME_BUFFER and list_positions():
                logging.info(sell_open(SYMBOL))
        else:
            logging.warning(f"Cannot get the last 15-minute delayed price for {SYMBOL}!")


if __name__ == "__main__":
    main()