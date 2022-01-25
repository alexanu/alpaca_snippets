# Source: https://github.com/alpacahq/alpaca-erasure
# Daily ranking by diff with EWA

import alpaca_trade_api as tradeapi
import pandas as pd
import numpy as np
from io import StringIO

from utils_for_alpaca import get_market_snapshot

from .Universes import Universe_SP500


NY = 'America/New_York'
alpaca = tradeapi.REST()




    price_df = get_market_snapshot(alpaca)
    diffs = {}
    param = 10
    for symbol in price_df.index:
        df = price_df[symbol]
        if len(df.close.values) <= param:
            continue
        ema = df.close.ewm(span=param).mean()[-1]
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