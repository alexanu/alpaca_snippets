
# Source: https://github.com/alpacahq/sp100algo
# Replication S&P100 index by buying the underlying stocks

import alpaca_trade_api as tradeapi

from bs4 import BeautifulSoup
import iexfinance as iex
import logging
import pandas as pd
import requests
import time

from .Universes import Universe_SP100

logger = logging.getLogger(__name__)
api = tradeapi.REST()


def submit_and_wait(orders, side):
    '''Submit orders and wait all of them go through.'''
    for symbol, qty in orders.items():
        try:
            api.submit_order(
                symbol=symbol,
                side=side,
                qty=qty,
                type='market', # place market orders at the market open
                time_in_force='day',
            )
        except Exception as e:
            logger.error(e)
    while True:
        orders = api.list_orders()
        if len(orders) == 0:
            break
        time.sleep(1)


def rebalance():
    '''Get up-to-date symbol list and calculate optimal portfolio, then
    trade accordingly.'''
    symbols = Universe_SP100
    partlen = 99
    result = {}
    for i in range(0, len(symbols), partlen):
        part = symbols[i:i + partlen]
        kstats = iex.Stock(part).get_key_stats()
        previous = iex.Stock(part).get_previous()
        for symbol in part:
            kstats[symbol].update(previous[symbol])
        result.update(kstats)
    stkdata = pd.DataFrame(result)

    weights = (stkdata.T['marketcap'] / stkdata.T['marketcap'].sum())
    pval = float(api.get_account().portfolio_value)

    target_qty = (pval * weights) // stkdata.T['close']
    current_qty = pd.Series({
        p.symbol: int(p.qty)
        for p in api.list_positions()})

    df = pd.DataFrame({
            'target_qty': target_qty,
            'current_qty': current_qty,
            'last_close': stkdata.T['close'],
            'weight': weights,
            'marketcap': stkdata.T['marketcap'],
        }).fillna(0)

    diff = df['target_qty'] - df['current_qty']

    # sell first, to have enough buying power back
    sells = {symbol: -int(qty) for symbol, qty in diff.items() if qty < 0}
    submit_and_wait(sells, 'sell')

    buys = {symbol: int(qty) for symbol, qty in diff.items() if qty > 0}
    submit_and_wait(buys, 'buy')


def main():
    '''The main loop. Perform rebalance() in the morning of
    market open day.
    '''
    open_dates = set([c._raw['date'] for c in api.get_calendar()])
    done = None
    while True:
        clock = api.get_clock()
        today = clock.timestamp.strftime('%Y-%m-%d')

        if today in open_dates and done != today:
            if clock.timestamp.time() >= pd.Timestamp('09:30').time(): # place market orders at the market open
                rebalance()
                done = today
        time.sleep(30)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()

