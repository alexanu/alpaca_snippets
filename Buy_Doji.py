
# Source: https://github.com/Jake0303/SimplePythonAlpacaStockTradingBot
# https://www.youtube.com/watch?v=9R7pCh4yCm8
# Buy a stock when a doji candle forms


import alpaca_trade_api as tradeapi
from alpaca_trade_api import StreamConn
import threading
import time
import datetime

from Alpaca_config import * # contains fmp key as well

class BuyDoji:
  def __init__(self):
    self.alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')
  def run(self):
        async def on_minute(conn, channel, bar): # every Minute
            symbol = bar.symbol
            print("Close: ", bar.close)
            print("Open: ", bar.open)
            print("Low: ", bar.low)
            print(symbol)
            #Check for Doji
            if bar.close > bar.open and bar.open - bar.low > 0.1:
                print('Buying on Doji!')
                self.alpaca.submit_order(symbol,1,'buy','market','day')
            #TODO : Take profit

        #Connect to get streaming market data
        conn = StreamConn('Polygon Key Here', 'Polygon Key Here', 'wss://alpaca.socket.polygon.io/stocks')
        on_minute = conn.on(r'AM$')(on_minute)
        conn.run(['AM.MSFT']) # Subscribe to Microsoft Stock

# Run the BuyDoji class
ls = BuyDoji()
ls.run()