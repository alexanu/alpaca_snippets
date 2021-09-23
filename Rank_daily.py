# Source: https://github.com/alpacahq/alpaca-erasure
# Daily ranking by diff with EWA

import alpaca_trade_api as tradeapi
import pandas as pd
import numpy as np
from io import StringIO

from .Universes import Universe_SP500


NY = 'America/New_York'
api = tradeapi.REST()


def _get_prices(symbols, end_dt, max_workers=5):

    start_dt = end_dt - pd.Timedelta('50 days')
    start = start_dt.isoformat()
    end = end_dt.isoformat()
    barset = {}
    idx = 0

    while idx <= len(symbols) - 1:
        for symbol in symbols[idx:idx+200]: # The maximum number of symbols we can request at once is 200
            bars = api.get_bars(symbol, TimeFrame.Day, start, end, limit=50, adjustment='raw')
            barset[symbol] = bars
        idx += 200

    return barset.df


def prices(symbols):
    '''Get the map of prices in DataFrame with the symbol name key.'''
    now = pd.Timestamp.now(tz=NY)
    end_dt = now
    if now.time() >= pd.Timestamp('09:30', tz=NY).time():
        end_dt = now - pd.Timedelta(now.strftime('%H:%M:%S')) - pd.Timedelta('1 minute')
    return _get_prices(symbols, end_dt)


def calc_scores(price_df, dayindex=-1):
    '''Calculate scores based on the indicator and return the sorted result.
    '''
    diffs = {}
    param = 10
    for symbol in price_df.columns.levels[0]:
        df = price_df[symbol]
        if len(df.close.values) <= param:
            continue
        ema = df.close.ewm(span=param).mean()[dayindex]
        last = df.close.values[dayindex]
        diff = (last - ema) / ema
        diffs[symbol] = diff

    df = pd.DataFrame(diffs.items(), columns=('symbol', 'score')).set_index('symbol')
    df = df[pd.notnull(df.score)]
    df.score = df.score / np.linalg.norm(df.score) * -0.5 + 0.5
    return df

def main():
    price_df = prices(Universe_SP500)
    df = calc_scores(price_df)

    buf = StringIO()
    print(df.to_csv(buf))
    print(buf.getvalue())


if __name__ == '__main__':
    main()