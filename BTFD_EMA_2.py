
# Source: https://github.com/alpacahq/samplealgo01
# Description:
'''
trades every day refreshing portfolio based on the EMA ranking
low [(price - EMA) vs price] ratio indicates there is a big dip in a short time
'''

import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame

import pandas as pd
import time
import logging

from Alpaca_config import * # contains fmp key as well

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

NY = 'America/New_York'

Universe = ...
position_size=100
max_positions=5

done = None
logging.info('start running')
clock = alpaca.get_clock()
now = clock.timestamp
if clock.is_open and done != now.strftime('%Y-%m-%d'):
    end_dt = pd.Timestamp.now(tz=NY)- pd.Timedelta('1 day')
    start_dt = end_dt - pd.Timedelta('50 days')
    barset = {}
    idx = 0
    while idx <= len(Universe) - 1: # takes around 2 minutes
        for symbol in Universe[idx:idx+200]: # The maximum number of symbols we can request at once is 200
            bars = alpaca.get_bars(symbol, TimeFrame.Day, start_dt.isoformat(), end_dt.isoformat(), limit=50, adjustment='raw').df
            barset[symbol] = bars
        idx += 200

    diffs = {}
    param = 10
    for symbol in barset.keys():
        try:
            df = barset[symbol]
            if len(df.close.values) <= param: # skip if not enough data points to calculate the indicator
                continue
            ema = df.close.ewm(span=param).mean()[-1] # last value of indicator
            last = df.close.values[-1] # latest price
            diff = (last - ema) / last
            diffs[symbol] = diff
        except:
            continue

    ranked = sorted(diffs.items(), key=lambda x: x[1])

    to_buy = set()
    to_sell = set()
    account = alpaca.get_account()

    for symbol, _ in ranked[:len(ranked) // 20]: # lowest 20
        price = float(barset[symbol].close.values[-1])
        if price > float(account.cash):  # excluding stocks too expensive to buy a share
            continue
        to_buy.add(symbol) # we wanna have stonks with big dips in a short time

    positions = alpaca.list_positions()
    logger.info(positions)
    holdings = {p.symbol: p for p in positions}
    holding_symbol = set(holdings.keys())
    to_sell = holding_symbol - to_buy # in pf, but not desired
    to_buy = to_buy - holding_symbol # not in pf, but desired

    orders = []
    for symbol in to_sell: # if a stock is in the pf, and not in the desired pf, ...
        shares = holdings[symbol].qty
        orders.append({'symbol': symbol,'qty': shares,'side': 'sell'}) # ... sell it
        logger.info(f'order(sell): {symbol} for {shares}')

    # likewise, if the portfoio is missing stocks from the
    # desired portfolio, buy them. We sent a limit for the total
    # position size so that we don't end up holding too many positions.
    max_to_buy = max_positions - (len(positions) - len(to_sell))
    for symbol in to_buy:
        if max_to_buy <= 0:
            break
        shares = position_size // float(barset[symbol].close.values[-1])
        if shares == 0.0:
            continue
        orders.append({'symbol': symbol,'qty': shares,'side': 'buy'})
        logger.info(f'order(buy): {symbol} for {shares}')
        max_to_buy -= 1


    # process the sell orders first
    wait = 30
        # This is where we actually submit the orders and wait for them to fill.
        #     Waiting is an important step since the orders aren't filled automatically,
        #     which means if your buys happen to come before your sells have filled,
        #     the buy orders will be bounced. In order to make the transition smooth,
        #     we sell first and wait for all the sell orders to fill before submitting
        #     our buy orders.

    sells = [o for o in orders if o['side'] == 'sell']
    for order in sells:
        try:
            logger.info(f'submit(sell): {order}')
            alpaca.submit_order(symbol=order['symbol'],qty=order['qty'],side='sell',type='market',time_in_force='day')
        except Exception as e:
            logger.error(e)
    count = wait
    while count > 0:
        pending = alpaca.list_orders()
        if len(pending) == 0:
            logger.info(f'all sell orders done')
            break
        logger.info(f'{len(pending)} sell orders pending...')
        time.sleep(1)
        count -= 1

    # process the buy orders next
    buys = [o for o in orders if o['side'] == 'buy']
    for order in buys:
        try:
            logger.info(f'submit(buy): {order}')
            alpaca.submit_order(symbol=order['symbol'],qty=order['qty'],side='buy',type='market',time_in_force='day')
        except Exception as e:
            logger.error(e)
    count = wait
    while count > 0:
        pending = alpaca.list_orders()
        if len(pending) == 0:
            logger.info(f'all buy orders done')
            break
        logger.info(f'{len(pending)} buy orders pending...')
        time.sleep(1)
        count -= 1

    done = now.strftime('%Y-%m-%d')
    logger.info(f'done for {done}')
