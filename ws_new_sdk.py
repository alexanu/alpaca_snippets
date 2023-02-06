# Alpaca office hours #2 video

from alpaca.data.live import CryptoDataStream
from talipp.indicators import EMA, ATR
from Alpaca_config import *


client = CryptoDataStream(API_KEY_PAPER,API_SECRET_PAPER)

bars = []
ema_2 = EMA(period = 2)
ema_4 = EMA(period = 4)
atr = ATR(period = 2)

async def bar_handler(data):
    ema_2.add_input_value(data.close)
    ema_4.add_input_value(data.close)
    print(ema_2,ema_4)
    atr.add_input_value(data)
    print(atr)

client.subscribe_bars(bar_handler,'BTC/USD','ETH/USD')
client.run()