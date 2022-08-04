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


diffs = {}
param = 10

for symbol in Universe_SP500:
    try:
        temp = alpaca.get_bars(symbol, TimeFrame.Day, n_days_ago, end_date, adjustment='raw').df
        temp.between_time('09:31', '16:00') # focus on market hours as for now trading on alpaca is restricted to market hours
        temp.index = temp.index.tz_localize(None) # remove +00:00 from datetime
        if len(temp.close.values) <= param:
            continue
        ema = temp.close.ewm(span=param).mean()[-1]
        last = temp.close.values[-1]
        diff = (last - ema) / ema
        diffs[symbol] = diff
    except:
        continue

df = pd.DataFrame(diffs.items(), columns=('symbol', 'score')).set_index('symbol')
df = df[pd.notnull(df.score)]
df.score = df.score / np.linalg.norm(df.score) * -0.5 + 0.5

