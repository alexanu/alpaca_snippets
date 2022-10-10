# 1) Select a ‘potential universe’ of stocks:
#       - tradable + shortable + marginable + easy-to-borrow
#       - exclude ETFs
#       - 10 < price < 1000
#       - 10 < price < 1000
#       - previous day notional trading volume (> $5M)
# 2) Before markets open, check the after hours number of trades and gain.
#       -- If there are at least 50 after hours trades:
#               --- choose the 15 largest gainers and open those long
#               --- choose the 15 largest losers and short those
#               --- all equally weight + bracket orders (market open, 3% stop loss, 12% take profit limit) to be submitted at market open.
# 3) Close any open positions at the end of the day.